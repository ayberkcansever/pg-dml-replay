package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
)

type ReadyForQueryMessage struct {
	Type   byte
	Length int32
	Status byte
}

func DecodeReadyForQueryMessage(pgPacketData []byte, readyForQueryMessage *ReadyForQueryMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	readyForQueryMessage.Type = protocol.ReadyForQuery
	readyForQueryMessage.Length = int32(messageLength)
	readyForQueryMessage.Status = pgPacketData[5]

	return lastIndex + 6
}
