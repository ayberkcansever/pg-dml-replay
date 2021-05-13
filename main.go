package main

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"log"
	"os"
)

var (
	iface       = "eth0"
	buffer      = int32(32896)
	filter      = "tcp and dst port 5000"
	kafkaBroker = "localhost:9092"
)
var preparedStatementMap map[string]protocol.ParseMessage
var kafkaProducer *kafka.Producer
var tcpPacketChannel chan gopacket.Packet
var dmlKafKaMessageChannel chan DmlQuery
var dmlKafkaTopic = "pg-dml-test"

type DmlQuery struct {
	Query      string           `json:"query"`
	Parameters []QueryParameter `json:"parameters"`
}

type QueryParameter struct {
	Type  int    `json:"type"`
	Value []byte `json:"value"`
}

func init() {
	argsWithProg := os.Args
	if len(argsWithProg) > 1 {
		iface = argsWithProg[1]
		filter = argsWithProg[2]
		kafkaBroker = argsWithProg[3]
	}

	tcpPacketChannel = make(chan gopacket.Packet)
	preparedStatementMap = make(map[string]protocol.ParseMessage)
	kafkaProducer, _ = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	dmlKafKaMessageChannel = make(chan DmlQuery)

	go startProcessingTcpPacket()
	go startInforming()
}

func main() {
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

	for packet := range source.Packets() {
		pgPacketData := packet.TransportLayer().LayerPayload()
		if len(pgPacketData) > 0 {
			tcpPacketChannel <- packet
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

func reassembly() []byte {
	packet := <-tcpPacketChannel
	pgPacketData := packet.TransportLayer().LayerPayload()
	return pgPacketData
}

func startProcessingTcpPacket() {
	packet := <-tcpPacketChannel
	pgPacketData := packet.TransportLayer().LayerPayload()

	messageType := pgPacketData[0]
	if protocol.IsKnownMessage(messageType) {
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
				newPacketData := reassembly()
				fullPacketData = append(fullPacketData, newPacketData...)
			} else {
				break
			}
		}

		lastIndex := 0
		var query string
		var queryParameters []QueryParameter
		for {
			messageData := fullPacketData[lastIndex:]
			messageType = messageData[0]

			if messageType == protocol.PARSE {
				var parseMessage protocol.ParseMessage
				msgLastIndex := protocol.DecodeParseMessage(messageData, &parseMessage)
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
				if bindMessage.IsPreparedStatement() {
					if parseMessage, ok := preparedStatementMap[bindMessage.Statement]; ok {
						query = parseMessage.Query
					}
				}
				lastIndex += messageLastIndex

				if query != "" {
					queryParameters = make([]QueryParameter, len(bindMessage.ParameterValues))
					for i, param := range bindMessage.ParameterValues {
						var queryParameter QueryParameter
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
				lastIndex += messageLastIndex
			} else if messageType == protocol.EXECUTE {
				var executeMessage protocol.ExecuteMessage
				messageLastIndex := protocol.DecodeExecuteMessage(messageData, &executeMessage)
				lastIndex += messageLastIndex
			} else if messageType == protocol.SYNC {
				var syncMessage protocol.SyncMessage
				messageLastIndex := protocol.DecodeSyncMessage(messageData, &syncMessage)
				lastIndex += messageLastIndex
			} else {
				break
			}

			if lastIndex >= len(fullPacketData) {
				if query != "" {
					dmlKafKaMessageChannel <- DmlQuery{
						Query:      query,
						Parameters: queryParameters,
					}
				}
				break
			}
		}
	}

	go startProcessingTcpPacket()
}

func startInforming() {
	for dmlQuery := range dmlKafKaMessageChannel {
		log.Println("Producing dml query: ", dmlQuery.Query)
		dmlJson, err := json.Marshal(dmlQuery)
		if err != nil {
			log.Println(err)
		} else {
			err = kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &dmlKafkaTopic},
				Value:          dmlJson,
			}, nil)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

/*func getInsertHex() []byte {
	hexString := "020000004500019100004000400600007f0000017f000001c7d9192089b9a8cf43f34b388018136aff8500000101080adee7c3d8dee7c3c150000000ba00696e7365727420696e746f2063616c6c5f6c6f67202863616c6c5f69642c2063616c6c65652c2063616c6c65722c20637265617465645f61742c207374617475732c20747970652c207569645f646174612c20757064617465645f61742c206964292076616c756573202824312c2024322c2024332c2024342c2024352c2024362c2024372c2024382c20243929000009000004130000041300000413000000000000041300000413000000000000000000000014420000008b00000009000000000000000000000000000000000001000900000003313233ffffffffffffffff0000001d323032312d30352d30392030393a33353a32372e3739343039392b3033000000064641494c4544ffffffff0000001b7b226d796b6579223a312c22796f75724b6579223a226b6579227dffffffff000000080000000000000004000044000000065000450000000900000000015300000004"
	decoded, _ := hex.DecodeString(hexString)
	return decoded
}*/
