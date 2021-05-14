package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
)

type NoDataMessage struct {
	Type   byte
	Length int32
}

func DecodeNoDataMessage(pgPacketData []byte, noDataMessage *NoDataMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	noDataMessage.Type = protocol.NoData
	noDataMessage.Length = int32(messageLength)

	return lastIndex + 5
}
