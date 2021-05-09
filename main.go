package main

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"log"
)

func main() {
	preparedStatementMap := make(map[string]protocol.ParseMessage)

	var (
		iface = "lo0"
		buffer = int32(16448)
		filter = "tcp and port 6432"
	)

	if !deviceExists(iface) {
		log.Fatal("Unable to open device ", iface)
	}

	handler, err := pcap.OpenLive(iface, buffer, false, pcap.BlockForever)
	if err != nil {
		log.Fatal(err)
	}
	defer handler.Close()

	if err := handler.SetBPFFilter(filter); err != nil {
		log.Fatal(err)
	}

	source := gopacket.NewPacketSource(handler, handler.LinkType())
	for packet := range source.Packets() {
		processPacket(packet, preparedStatementMap)
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

func processPacket(packet gopacket.Packet, preparedStatementMap map[string]protocol.ParseMessage) {
	fullPacketData := packet.Data()
	if len(fullPacketData) <= 56 {
		return
	}

	pgPacketData := fullPacketData[56:]

	lastIndex := 0
	for {
		messageData := pgPacketData[lastIndex:]
		messageType := messageData[0]
		if messageType == protocol.PARSE {
			var parseMessage protocol.ParseMessage
			msgLastIndex := protocol.DecodeParseMessage(messageData, &parseMessage)
			if parseMessage.IsDMLQuery() && parseMessage.Statement != "" {
				preparedStatementMap[parseMessage.Statement] = parseMessage
				fmt.Println("Statement saved -> ")
			}
			lastIndex += msgLastIndex
			fmt.Println("Parse: " + parseMessage.String())
		} else if messageType == protocol.BIND {
			var bindMessage protocol.BindMessage
			messageLastIndex := protocol.DecodeBindMessage(messageData, &bindMessage)
			if bindMessage.IsPreparedStatement() {
				if parseMessage, ok := preparedStatementMap[bindMessage.Statement]; ok {
					fmt.Println("Query Bind: " + parseMessage.Query)
				}
			}
			lastIndex += messageLastIndex
			fmt.Println("Bind: " + bindMessage.String())
		} else if messageType == protocol.DESCRIBE {
			var describeMessage protocol.DescribeMessage
			messageLastIndex := protocol.DecodeDescribeMessage(messageData, &describeMessage)
			lastIndex += messageLastIndex
			fmt.Println("Describe: " + describeMessage.String())
		} else if messageType == protocol.EXECUTE {
			var executeMessage protocol.ExecuteMessage
			messageLastIndex := protocol.DecodeExecuteMessage(messageData, &executeMessage)
			lastIndex += messageLastIndex
			fmt.Println("Execute: " + executeMessage.String())
		} else if messageType == protocol.SYNC {
			var syncMessage protocol.SyncMessage
			messageLastIndex := protocol.DecodeSyncMessage(messageData, &syncMessage)
			lastIndex += messageLastIndex
			fmt.Println("Sync: " + syncMessage.String())
		} else {
			break
		}

		if lastIndex >= len(pgPacketData) {
			break
		}
	}
}

/*func getInsertHex() []byte {
	hexString := "020000004500019100004000400600007f0000017f000001c7d9192089b9a8cf43f34b388018136aff8500000101080adee7c3d8dee7c3c150000000ba00696e7365727420696e746f2063616c6c5f6c6f67202863616c6c5f69642c2063616c6c65652c2063616c6c65722c20637265617465645f61742c207374617475732c20747970652c207569645f646174612c20757064617465645f61742c206964292076616c756573202824312c2024322c2024332c2024342c2024352c2024362c2024372c2024382c20243929000009000004130000041300000413000000000000041300000413000000000000000000000014420000008b00000009000000000000000000000000000000000001000900000003313233ffffffffffffffff0000001d323032312d30352d30392030393a33353a32372e3739343039392b3033000000064641494c4544ffffffff0000001b7b226d796b6579223a312c22796f75724b6579223a226b6579227dffffffff000000080000000000000004000044000000065000450000000900000000015300000004"
	decoded, _ := hex.DecodeString(hexString)
	return decoded
}*/
