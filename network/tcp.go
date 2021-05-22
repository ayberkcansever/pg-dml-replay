package network

import (
	"com.canseverayberk/pg-dml-replay/db"
	"com.canseverayberk/pg-dml-replay/kafka"
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/protocol/incoming"
	"com.canseverayberk/pg-dml-replay/protocol/outgoing"
	"com.canseverayberk/pg-dml-replay/util"
	"encoding/binary"
	"fmt"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"log"
	"strconv"
	"strings"
)

var preparedStatementMap = make(map[string]outgoing.ParseMessage)
var iface string
var buffer = int32(262144)
var filter string
var tcpOutgoingPacketChannel = make(chan gopacket.Packet)
var tcpIncomingPacketChannel = make(chan gopacket.Packet)
var messageQueue = goconcurrentqueue.NewFIFO()
var port uint64
var portTcpMessageQueueMap = make(map[string]*goconcurrentqueue.FIFO)
var portIncomingSeqMap = make(map[string]uint32)

func StartListeningPackets() {
	iface = util.GetEnv("IFACE", "eth0")
	filter = util.GetEnv("FILTER", "tcp and port 5000")
	port, _ = strconv.ParseUint(strings.Split(filter, "port ")[1], 10, 32)

	if !deviceExists(iface) {
		log.Fatal("Unable to open device ", iface)
	}
	fmt.Printf("Device opened: %s with filter: %s\n", iface, filter)

	handler, err := pcap.OpenLive(iface, buffer, false, pcap.BlockForever)
	if err != nil {
		log.Fatal(err)
	}
	defer handler.Close()

	if err := handler.SetBPFFilter(filter); err != nil {
		log.Fatal(err)
	}

	go startProcessingOutgoingTcpPackets()
	go startProcessingIncomingTcpPackets()

	source := gopacket.NewPacketSource(handler, handler.LinkType())
	source.DecodeOptions.Lazy = true
	source.NoCopy = true
	source.DecodeStreamsAsDatagrams = true

	for packet := range source.Packets() {
		pgPacketData := packet.TransportLayer().LayerPayload()
		if len(pgPacketData) > 0 {
			tcpLayer := packet.Layer(layers.LayerTypeTCP)
			tcp, _ := tcpLayer.(*layers.TCP)
			if tcp.DstPort == layers.TCPPort(uint16(port)) {
				tcpOutgoingPacketChannel <- packet
			} else {
				if tcp.Seq > portIncomingSeqMap[tcp.DstPort.String()] {
					portIncomingSeqMap[tcp.DstPort.String()] = tcp.Seq
					tcpIncomingPacketChannel <- packet
				}
			}
		}
	}
}

func startProcessingIncomingTcpPackets() {
	packet := <-tcpIncomingPacketChannel
	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	tcp, _ := tcpLayer.(*layers.TCP)
	pgPacketData := packet.TransportLayer().LayerPayload()

	messageType := pgPacketData[0]
	if protocol.IsKnownIncomingMessage(messageType) {
		processIncomingMessage(pgPacketData, tcp.DstPort.String())
	}
	startProcessingIncomingTcpPackets()
}

func startProcessingOutgoingTcpPackets() {
	packet := <-tcpOutgoingPacketChannel
	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	tcp, _ := tcpLayer.(*layers.TCP)
	pgPacketData := packet.TransportLayer().LayerPayload()

	messageType := pgPacketData[0]
	if protocol.IsKnownOutgoingMessage(messageType) {
		processOutgoingMessage(pgPacketData, tcp.SrcPort.String())
	}
	startProcessingOutgoingTcpPackets()
}

func processIncomingMessage(pgPacketData []byte, dstPort string) {
	var fullPacketData = make([]byte, 0)
	lengthData := []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := int(binary.BigEndian.Uint32(lengthData))
	successivePacketData := make([]byte, 0)

	if len(pgPacketData) > messageLength {
		fullPacketData = append(fullPacketData, pgPacketData[0:(messageLength+1)]...)
		successivePacketData = pgPacketData[(messageLength + 1):]
	processIndividualPgPacket:
		for {
			if len(successivePacketData) == 0 {
				break
			}

			if len(successivePacketData) < 5 {
				successivePacketData = append(successivePacketData, reassemblyIncoming()...)
				break processIndividualPgPacket
			}
			lengthData = []byte{successivePacketData[1], successivePacketData[2], successivePacketData[3], successivePacketData[4]}
			messageLength := int(binary.BigEndian.Uint32(lengthData))
			for {
				if len(successivePacketData) < messageLength {
					break
				}
				if len(successivePacketData) == messageLength {
					break processIndividualPgPacket
				}
				fullPacketData = append(fullPacketData, successivePacketData[0:(messageLength+1)]...)
				successivePacketData = pgPacketData[len(fullPacketData):]
				if len(successivePacketData) == 0 {
					break processIndividualPgPacket
				}
				if len(successivePacketData) < 5 {
					successivePacketData = append(successivePacketData, reassemblyIncoming()...)
					break processIndividualPgPacket
				}
				lengthData = []byte{successivePacketData[1], successivePacketData[2], successivePacketData[3], successivePacketData[4]}
				messageLength = int(binary.BigEndian.Uint32(lengthData))
			}

			if len(successivePacketData) > 0 && messageLength > len(successivePacketData) {
				successivePacketData = append(successivePacketData, reassemblyIncoming()...)
				break processIndividualPgPacket
			}
		}
	} else {
		fullPacketData = pgPacketData
		lengthData := []byte{fullPacketData[1], fullPacketData[2], fullPacketData[3], fullPacketData[4]}
		messageLength := int(binary.BigEndian.Uint32(lengthData))
		for {
			if len(fullPacketData) < messageLength+1 {
				successivePacketData = fullPacketData
				successivePacketData = append(successivePacketData, reassemblyIncoming()...)
				lengthData = []byte{successivePacketData[1], successivePacketData[2], successivePacketData[3], successivePacketData[4]}
				messageLength = int(binary.BigEndian.Uint32(lengthData))
				if len(successivePacketData) < messageLength {
					fullPacketData = successivePacketData
				} else {
					fullPacketData = successivePacketData[0:(messageLength + 1)]
				}
				successivePacketData = successivePacketData[len(fullPacketData):]
			} else {
				break
			}
		}
	}

	lastIndex := 0
	for {
		messageData := fullPacketData[lastIndex:]
		messageType := messageData[0]
		lengthData = []byte{messageData[1], messageData[2], messageData[3], messageData[4]}
		messageLength = int(binary.BigEndian.Uint32(lengthData))

		if messageType == protocol.ParseComplete {
			var parseCompleteMessage incoming.ParseCompleteMessage
			msgLastIndex := incoming.DecodeParseCompleteMessage(messageData, &parseCompleteMessage)
			lastIndex += msgLastIndex
			fmt.Println("<------ParseComplete")
		} else if messageType == protocol.BindComplete {
			var bindCompleteMessage incoming.BindCompleteMessage
			msgLastIndex := incoming.DecodeBindCompleteMessage(messageData, &bindCompleteMessage)
			lastIndex += msgLastIndex
			fmt.Println("<------BindComplete")
		} else if messageType == protocol.CommandComplete {
			var commandCompleteMessage incoming.CommandCompleteMessage
			msgLastIndex := incoming.DecodeCommandCompleteMessage(messageData, &commandCompleteMessage)
			lastIndex += msgLastIndex
			fmt.Println("<------CommandComplete: ", commandCompleteMessage.Tag)
			if commandCompleteMessage.IsCommitMessage() {
				for {
					dmlQuery, _ := portTcpMessageQueueMap[dstPort].Dequeue()
					if dmlQuery == nil {
						break
					}
					kafka.DmlKafKaMessageChannel <- dmlQuery.(db.DmlQuery)
					fmt.Println("Queue size: ", dstPort, portTcpMessageQueueMap[dstPort].GetLen())
				}
			}
		} else if messageType == protocol.EmptyQueryResponse {
			var emptyQueryResponseMessage incoming.EmptyQueryResponseMessage
			msgLastIndex := incoming.DecodeEmptyQueryResponse(messageData, &emptyQueryResponseMessage)
			lastIndex += msgLastIndex
			fmt.Println("<------EmptyQueryResponse")
		} else if messageType == protocol.NoData {
			var noDataMessage incoming.NoDataMessage
			msgLastIndex := incoming.DecodeNoDataMessage(messageData, &noDataMessage)
			lastIndex += msgLastIndex
			fmt.Println("<------NoData")
		} else if messageType == protocol.ReadyForQuery {
			var readyForQueryMessage incoming.ReadyForQueryMessage
			msgLastIndex := incoming.DecodeReadyForQueryMessage(messageData, &readyForQueryMessage)
			lastIndex += msgLastIndex
			fmt.Println("<------ReadyForQuery")
		} else if messageType == protocol.DataRow {
			var dataRowMessage incoming.DataRowMessage
			msgLastIndex := incoming.DecodeDataRowMessage(messageData, &dataRowMessage)
			lastIndex += msgLastIndex
			fmt.Println("<------DataRow")
		} else {
			break
		}

		if lastIndex >= len(fullPacketData) {
			break
		}
	}

	if len(successivePacketData) > 0 {
		processIncomingMessage(successivePacketData, dstPort)
	}
}

func processOutgoingMessage(pgPacketData []byte, srcPort string) {
	var fullPacketData = make([]byte, 0)
	lengthData := []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := int(binary.BigEndian.Uint32(lengthData))
	successivePacketData := make([]byte, 0)

	if len(pgPacketData) > messageLength {
		fullPacketData = append(fullPacketData, pgPacketData[0:(messageLength+1)]...)
		successivePacketData = pgPacketData[(messageLength + 1):]
	processIndividualPgPacket:
		for {
			if len(successivePacketData) == 0 {
				break
			}

			if len(successivePacketData) < 5 {
				successivePacketData = append(successivePacketData, reassemblyOutgoing()...)
				break processIndividualPgPacket
			}
			lengthData = []byte{successivePacketData[1], successivePacketData[2], successivePacketData[3], successivePacketData[4]}
			messageLength := int(binary.BigEndian.Uint32(lengthData))
			for {
				if len(successivePacketData) < messageLength {
					break
				}
				if len(successivePacketData) == messageLength {
					break processIndividualPgPacket
				}
				fullPacketData = append(fullPacketData, successivePacketData[0:(messageLength+1)]...)
				successivePacketData = pgPacketData[len(fullPacketData):]
				if len(successivePacketData) == 0 {
					break processIndividualPgPacket
				}
				if len(successivePacketData) < 5 {
					successivePacketData = append(successivePacketData, reassemblyOutgoing()...)
					break processIndividualPgPacket
				}
				lengthData = []byte{successivePacketData[1], successivePacketData[2], successivePacketData[3], successivePacketData[4]}
				messageLength = int(binary.BigEndian.Uint32(lengthData))
			}

			if len(successivePacketData) > 0 && messageLength > len(successivePacketData) {
				successivePacketData = append(successivePacketData, reassemblyOutgoing()...)
				break
			}
		}
	} else {
		fullPacketData = pgPacketData
		lengthData := []byte{fullPacketData[1], fullPacketData[2], fullPacketData[3], fullPacketData[4]}
		messageLength := int(binary.BigEndian.Uint32(lengthData))
		for {
			if len(fullPacketData) < messageLength+1 {
				successivePacketData = fullPacketData
				successivePacketData = append(successivePacketData, reassemblyOutgoing()...)
				lengthData = []byte{successivePacketData[1], successivePacketData[2], successivePacketData[3], successivePacketData[4]}
				messageLength = int(binary.BigEndian.Uint32(lengthData))
				if len(successivePacketData) < messageLength {
					fullPacketData = successivePacketData
				} else {
					fullPacketData = successivePacketData[0:(messageLength + 1)]
				}
				successivePacketData = successivePacketData[len(fullPacketData):]
			} else {
				break
			}
		}
	}

	lastIndex := 0
	var query string
	var queryParameters []db.QueryParameter
	for {
		messageData := fullPacketData[lastIndex:]
		messageType := messageData[0]

		if messageType == protocol.Parse {
			var parseMessage outgoing.ParseMessage
			msgLastIndex := outgoing.DecodeParseMessage(messageData, &parseMessage)
			_ = messageQueue.Enqueue(parseMessage)
			if parseMessage.IsDMLQuery() && parseMessage.Statement != "" {
				preparedStatementMap[parseMessage.Statement] = parseMessage
			}
			lastIndex += msgLastIndex
			if parseMessage.Query != "" && parseMessage.IsDMLQuery() {
				query = parseMessage.Query
			}
			fmt.Println("------>Parse")
		} else if messageType == protocol.Bind {
			var bindMessage outgoing.BindMessage
			messageLastIndex := outgoing.DecodeBindMessage(messageData, &bindMessage)
			_ = messageQueue.Enqueue(bindMessage)
			if bindMessage.IsPreparedStatement() {
				if parseMessage, ok := preparedStatementMap[bindMessage.Statement]; ok {
					query = parseMessage.Query
				}
			}
			lastIndex += messageLastIndex

			if query != "" {
				queryParameters = make([]db.QueryParameter, len(bindMessage.ParameterValues))
				for i, param := range bindMessage.ParameterValues {
					var queryParameter db.QueryParameter
					if bindMessage.ParameterFormats[i] == 0 {
						queryParameter.Type = 0
					} else {
						queryParameter.Type = 1
					}
					queryParameter.Value = param
					queryParameters[i] = queryParameter
				}
			}
			fmt.Println("------>Bind")
		} else if messageType == protocol.Describe {
			var describeMessage outgoing.DescribeMessage
			messageLastIndex := outgoing.DecodeDescribeMessage(messageData, &describeMessage)
			_ = messageQueue.Enqueue(describeMessage)
			lastIndex += messageLastIndex
			fmt.Println("------>Describe")
		} else if messageType == protocol.Execute {
			var executeMessage outgoing.ExecuteMessage
			messageLastIndex := outgoing.DecodeExecuteMessage(messageData, &executeMessage)
			_ = messageQueue.Enqueue(executeMessage)
			lastIndex += messageLastIndex
			fmt.Println("------>Execute")
		} else if messageType == protocol.Sync {
			var syncMessage outgoing.SyncMessage
			messageLastIndex := outgoing.DecodeSyncMessage(messageData, &syncMessage)
			_ = messageQueue.Enqueue(syncMessage)
			lastIndex += messageLastIndex
			fmt.Println("------>Sync")
		} else {
			break
		}

		if lastIndex >= len(fullPacketData) {
			if query != "" {
				_ = getSrcPortQueue(srcPort).Enqueue(db.DmlQuery{
					Query:      query,
					Parameters: queryParameters,
				})
				fmt.Println("Queue size: ", srcPort, portTcpMessageQueueMap[srcPort].GetLen())
			}
			break
		}
	}

	if len(successivePacketData) > 0 {
		processOutgoingMessage(successivePacketData, srcPort)
	}
}

func reassemblyIncoming() []byte {
	packet := <-tcpIncomingPacketChannel
	return packet.TransportLayer().LayerPayload()
}

func reassemblyOutgoing() []byte {
	packet := <-tcpOutgoingPacketChannel
	return packet.TransportLayer().LayerPayload()
}

func deviceExists(name string) bool {
	devices, err := pcap.FindAllDevs()
	if err != nil {
		log.Panic(err)
	}
	for _, device := range devices {
		if device.Name == name {
			return true
		}
	}
	return false
}

func getSrcPortQueue(srcPort string) *goconcurrentqueue.FIFO {
	queue := portTcpMessageQueueMap[srcPort]
	if queue == nil {
		queue = goconcurrentqueue.NewFIFO()
		portTcpMessageQueueMap[srcPort] = queue
	}
	return queue
}
