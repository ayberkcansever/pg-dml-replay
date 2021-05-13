package protocol

import (
	"encoding/binary"
)

type DescribeMessage struct {
	Type         byte
	Length       int32
	DescribeType string
	Statement    string
}

func DecodeDescribeMessage(pgPacketData []byte, describeMessage *DescribeMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	describeType := string(pgPacketData[5])

	statementStartIndex := 6
	statementEndIndex := statementStartIndex
	for {
		if pgPacketData[statementEndIndex] == 0 {
			statementEndIndex++
			break
		}
		statementEndIndex++
	}
	statement := string(pgPacketData[statementStartIndex : statementEndIndex-1])

	describeMessage.Type = 44
	describeMessage.Length = int32(messageLength)
	describeMessage.DescribeType = describeType
	describeMessage.Statement = statement

	return statementEndIndex
}
