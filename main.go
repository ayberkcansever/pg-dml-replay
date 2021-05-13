package main

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"log"
	"os"
)

var preparedStatementMap map[string]protocol.ParseMessage

func main() {
	preparedStatementMap = make(map[string]protocol.ParseMessage)

	argsWithProg := os.Args

	var (
		iface  = "eth0"
		buffer = int32(32896)
		filter = "tcp and dst port 5000"
	)

	if len(argsWithProg) > 1 {
		iface = argsWithProg[1]
		filter = argsWithProg[2]
	}

	if !deviceExists(iface) {
		log.Fatal("Unable to open device ", iface)
	}
	log.Println("Device opened: ", iface, filter)

	handler, err := pcap.OpenLive(iface, buffer, false, pcap.BlockForever)
	if err != nil {
		log.Fatal(err)
	}
	defer handler.Close()

	if err := handler.SetBPFFilter(filter); err != nil {
		log.Fatal(err)
	}

	source := gopacket.NewPacketSource(handler, handler.LinkType())
	source.DecodeStreamsAsDatagrams = true

	packetChannel := make(chan gopacket.Packet)
	go processPacket(packetChannel)

	for packet := range source.Packets() {
		pgPacketData := packet.TransportLayer().LayerPayload()
		if len(pgPacketData) > 0 {
			packetChannel <- packet
		}
	}

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

func reassembly(packetChannel chan gopacket.Packet) []byte {
	packet := <-packetChannel
	pgPacketData := packet.TransportLayer().LayerPayload()
	return pgPacketData
}

func processPacket(packetChannel chan gopacket.Packet) {
	packet := <-packetChannel
	pgPacketData := packet.TransportLayer().LayerPayload()

	messageType := pgPacketData[0]
	if protocol.IsKnownMessage(messageType) {
		var fullPacketData = make([]byte, 0)
		lengthData := []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
		messageLength := int(binary.BigEndian.Uint32(lengthData))
		if messageLength > 1000000 {
			fmt.Print(messageLength)
		}
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
				newPacketData := reassembly(packetChannel)
				fullPacketData = append(fullPacketData, newPacketData...)
			} else {
				break
			}
		}

		lastIndex := 0
		for {
			messageData := fullPacketData[lastIndex:]
			messageType := messageData[0]

			if messageType == protocol.PARSE {
				var parseMessage protocol.ParseMessage
				msgLastIndex := protocol.DecodeParseMessage(messageData, &parseMessage)
				if parseMessage.IsDMLQuery() && parseMessage.Statement != "" {
					preparedStatementMap[parseMessage.Statement] = parseMessage
					log.Println("Statement saved -> ")
				}
				lastIndex += msgLastIndex
				log.Println("Parse: " + parseMessage.String())
			} else if messageType == protocol.BIND {
				var bindMessage protocol.BindMessage
				messageLastIndex := protocol.DecodeBindMessage(messageData, &bindMessage)
				if bindMessage.IsPreparedStatement() {
					if parseMessage, ok := preparedStatementMap[bindMessage.Statement]; ok {
						log.Println("Query Bind: " + parseMessage.Query)
					}
				}
				lastIndex += messageLastIndex
				log.Println("Bind: " + bindMessage.String())
			} else if messageType == protocol.DESCRIBE {
				var describeMessage protocol.DescribeMessage
				messageLastIndex := protocol.DecodeDescribeMessage(messageData, &describeMessage)
				lastIndex += messageLastIndex
				log.Println("Describe: " + describeMessage.String())
			} else if messageType == protocol.EXECUTE {
				var executeMessage protocol.ExecuteMessage
				messageLastIndex := protocol.DecodeExecuteMessage(messageData, &executeMessage)
				lastIndex += messageLastIndex
				log.Println("Execute: " + executeMessage.String())
			} else if messageType == protocol.SYNC {
				var syncMessage protocol.SyncMessage
				messageLastIndex := protocol.DecodeSyncMessage(messageData, &syncMessage)
				lastIndex += messageLastIndex
				log.Println("Sync: " + syncMessage.String())
			} else {
				break
			}

			if lastIndex >= len(fullPacketData) {
				break
			}
		}
	}

	go processPacket(packetChannel)
}

/*func getInsertHex() []byte {
	hexString := "020000004500019100004000400600007f0000017f000001c7d9192089b9a8cf43f34b388018136aff8500000101080adee7c3d8dee7c3c150000000ba00696e7365727420696e746f2063616c6c5f6c6f67202863616c6c5f69642c2063616c6c65652c2063616c6c65722c20637265617465645f61742c207374617475732c20747970652c207569645f646174612c20757064617465645f61742c206964292076616c756573202824312c2024322c2024332c2024342c2024352c2024362c2024372c2024382c20243929000009000004130000041300000413000000000000041300000413000000000000000000000014420000008b00000009000000000000000000000000000000000001000900000003313233ffffffffffffffff0000001d323032312d30352d30392030393a33353a32372e3739343039392b3033000000064641494c4544ffffffff0000001b7b226d796b6579223a312c22796f75724b6579223a226b6579227dffffffff000000080000000000000004000044000000065000450000000900000000015300000004"
	decoded, _ := hex.DecodeString(hexString)
	return decoded
}*/
