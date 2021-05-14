package outgoing

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
)

type SyncMessage struct {
	Type   byte
	Length int32
}

func DecodeSyncMessage(pgPacketData []byte, syncMessage *SyncMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	syncMessage.Type = protocol.Sync
	syncMessage.Length = int32(messageLength)

	return lastIndex + 5
}
