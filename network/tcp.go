package network

import (
	"com.canseverayberk/pg-dml-replay/db"
	"com.canseverayberk/pg-dml-replay/kafka"
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/util"
	"encoding/binary"
	"fmt"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"log"
)

var preparedStatementMap = make(map[string]protocol.ParseMessage)
var iface string
var buffer = int32(32896)
var filter string
var tcpPacketChannel = make(chan gopacket.Packet)
var messageQueue = goconcurrentqueue.NewFIFO()

func StartListeningPackets() {
	iface = util.GetEnv("IFACE", "eth0")
	filter = util.GetEnv("FILTER", "tcp and dst port 5000")

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

	go startProcessingTcpPackets()

	source := gopacket.NewPacketSource(handler, handler.LinkType())
	source.DecodeStreamsAsDatagrams = true

	for packet := range source.Packets() {
		pgPacketData := packet.TransportLayer().LayerPayload()
		if len(pgPacketData) > 0 {
			tcpPacketChannel <- packet
		}
	}
}

func startProcessingTcpPackets() {
	packet := <-tcpPacketChannel
	pgPacketData := packet.TransportLayer().LayerPayload()

	messageType := pgPacketData[0]
	if protocol.IsKnownOutgoingMessage(messageType) {
		processMessage(pgPacketData)
	}

	go startProcessingTcpPackets()
}

func processMessage(pgPacketData []byte) {
	var fullPacketData = make([]byte, 0)
	lengthData := []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := int(binary.BigEndian.Uint32(lengthData))

	if len(pgPacketData) > messageLength {
		fullPacketData = append(fullPacketData, pgPacketData[0:(messageLength+1)]...)
		successivePacketData := pgPacketData[(messageLength + 1):]
		for {
			if len(successivePacketData) == 0 {
				break
			}

			lengthData = []byte{successivePacketData[1], successivePacketData[2], successivePacketData[3], successivePacketData[4]}
			messageLength = int(binary.BigEndian.Uint32(lengthData))
			if len(successivePacketData) > messageLength {
				fullPacketData = append(fullPacketData, successivePacketData[0:(messageLength+1)]...)
				successivePacketData = pgPacketData[len(fullPacketData):]
			}
			if messageLength > len(pgPacketData) {
				fullPacketData = make([]byte, 0)
				break
			}
		}
	}
	if len(pgPacketData) <= messageLength {
		fullPacketData = append(fullPacketData, pgPacketData...)
	}
	for {
		if len(fullPacketData) <= messageLength {
			fullPacketData = append(fullPacketData, reassembly()...)
		} else {
			break
		}
	}

	lastIndex := 0
	var query string
	var queryParameters []db.QueryParameter
	for {
		messageData := fullPacketData[lastIndex:]
		messageType := messageData[0]

		if messageType == protocol.PARSE {
			var parseMessage protocol.ParseMessage
			msgLastIndex := protocol.DecodeParseMessage(messageData, &parseMessage)
			_ = messageQueue.Enqueue(parseMessage)
			if parseMessage.IsDMLQuery() && parseMessage.Statement != "" {
				preparedStatementMap[parseMessage.Statement] = parseMessage
			}
			lastIndex += msgLastIndex
			if parseMessage.Query != "" && parseMessage.IsDMLQuery() {
				query = parseMessage.Query
			}
		} else if messageType == protocol.BIND {
			var bindMessage protocol.BindMessage
			messageLastIndex := protocol.DecodeBindMessage(messageData, &bindMessage)
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
		} else if messageType == protocol.DESCRIBE {
			var describeMessage protocol.DescribeMessage
			messageLastIndex := protocol.DecodeDescribeMessage(messageData, &describeMessage)
			_ = messageQueue.Enqueue(describeMessage)
			lastIndex += messageLastIndex
		} else if messageType == protocol.EXECUTE {
			var executeMessage protocol.ExecuteMessage
			messageLastIndex := protocol.DecodeExecuteMessage(messageData, &executeMessage)
			_ = messageQueue.Enqueue(executeMessage)
			lastIndex += messageLastIndex
		} else if messageType == protocol.SYNC {
			var syncMessage protocol.SyncMessage
			messageLastIndex := protocol.DecodeSyncMessage(messageData, &syncMessage)
			_ = messageQueue.Enqueue(syncMessage)
			lastIndex += messageLastIndex
		} else {
			break
		}

		if lastIndex >= len(fullPacketData) {
			if query != "" {
				kafka.DmlKafKaMessageChannel <- db.DmlQuery{
					Query:      query,
					Parameters: queryParameters,
				}
			}
			break
		}
	}
}

func reassembly() []byte {
	packet := <-tcpPacketChannel
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
