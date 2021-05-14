package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
)

type EmptyQueryResponseMessage struct {
	Type   byte
	Length int32
}

func DecodeEmptyQueryResponse(pgPacketData []byte, emptyQueryResponseMessage *EmptyQueryResponseMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	emptyQueryResponseMessage.Type = protocol.EmptyQueryResponse
	emptyQueryResponseMessage.Length = int32(messageLength)

	return lastIndex + 5
}
