package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
)

type BindCompleteMessage struct {
	Type   byte
	Length int32
}

func DecodeBindCompleteMessage(pgPacketData []byte, bindComplete *BindCompleteMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	bindComplete.Type = protocol.BindComplete
	bindComplete.Length = int32(messageLength)

	return lastIndex + 5
}
