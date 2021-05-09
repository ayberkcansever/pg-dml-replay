package protocol

import (
	"encoding/binary"
	"fmt"
)

type DescribeMessage struct {
	Type         byte
	Length       int32
	DescribeType string
	Statement    string
}

func (m DescribeMessage) String() string {
	return fmt.Sprintf("[%b, %d, %s]", int32(m.Type), m.Length, m.Statement)
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
