package protocol

import (
	"encoding/binary"
)

type SyncMessage struct {
	Type   byte
	Length int32
}

func DecodeSyncMessage(pgPacketData []byte, syncMessage *SyncMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	syncMessage.Type = 83
	syncMessage.Length = int32(messageLength)

	return lastIndex + 5
}
