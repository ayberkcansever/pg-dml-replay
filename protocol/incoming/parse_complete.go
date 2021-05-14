package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
)

type ParseCompleteMessage struct {
	Type   byte
	Length int32
}

func DecodeParseCompleteMessage(pgPacketData []byte, parseComplete *ParseCompleteMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	parseComplete.Type = protocol.ParseComplete
	parseComplete.Length = int32(messageLength)

	return lastIndex + 5
}
