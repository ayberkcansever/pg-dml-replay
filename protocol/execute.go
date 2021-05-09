package protocol

import (
	"encoding/binary"
	"fmt"
)

type ExecuteMessage struct {
	Type             byte
	Length           int32
	Portal           string
	RowCountToReturn int32
}

func (m ExecuteMessage) String() string {
	return fmt.Sprintf("[%b, %d, %s, %d]", int32(m.Type), m.Length, m.Portal, m.RowCountToReturn)
}

func DecodeExecuteMessage(pgPacketData []byte, executeMessage *ExecuteMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	portalStartIndex := 5
	portalEndIndex := portalStartIndex
	for {
		if pgPacketData[portalEndIndex] == 0 {
			portalEndIndex++
			break
		}
		portalEndIndex++
	}
	portal := string(pgPacketData[portalStartIndex : portalEndIndex-1])

	var rowCountToReturn = binary.BigEndian.Uint32([]byte{pgPacketData[portalEndIndex], pgPacketData[portalEndIndex+1], pgPacketData[portalEndIndex+2], pgPacketData[portalEndIndex+3]})

	executeMessage.Type = 44
	executeMessage.Length = int32(messageLength)
	executeMessage.Portal = portal
	executeMessage.RowCountToReturn = int32(rowCountToReturn)

	return portalEndIndex + 4
}