package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type BindMessage struct {
	Type                   byte
	Length                 int32
	Portal                 string
	Statement              string
	ParameterFormatsLength int16
	ParameterFormats       []int16
	ParameterValuesLength  int16
	ParameterValues        [][]byte
	ResultFormats          int16
	ResultFormatValues     []int16
}

func (m BindMessage) String() string {
	return fmt.Sprintf("[%b, %d, %s, %s, %d, %b, %d, %s, %d]",
		int32(m.Type), m.Length, m.Portal, m.Statement, m.ParameterFormatsLength, m.ParameterFormats,
		m.ParameterValuesLength, m.ParameterValues, m.ResultFormats)
}

func (m BindMessage) IsPreparedStatement() bool {
	return m.Statement != ""
}

func DecodeBindMessage(pgPacketData []byte, bindMessage *BindMessage) (lastIndex int) {
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

	statementStartIndex := portalEndIndex
	statementEndIndex := statementStartIndex
	for {
		if pgPacketData[statementEndIndex] == 0 {
			statementEndIndex++
			break
		}
		statementEndIndex++
	}
	statement := string(pgPacketData[statementStartIndex : statementEndIndex-1])

	parameterFormatsLengthStartIndex := statementEndIndex
	parameterFormatsLengthEndIndex := parameterFormatsLengthStartIndex + 2
	parameterFormatsLength := binary.BigEndian.Uint16(pgPacketData[parameterFormatsLengthStartIndex:parameterFormatsLengthEndIndex])

	parameterFormatsIndex := parameterFormatsLengthEndIndex
	parameterFormats := make([]int16, parameterFormatsLength)
	for i := 0; i < int(parameterFormatsLength); i++ {
		parameterFormats[i] = int16(binary.BigEndian.Uint16(pgPacketData[parameterFormatsIndex : parameterFormatsIndex+2]))
		parameterFormatsIndex = parameterFormatsIndex + 2
	}

	parameterValuesLengthStartIndex := parameterFormatsIndex
	parameterValuesLengthEndIndex := parameterValuesLengthStartIndex + 2
	parameterValuesLength := binary.BigEndian.Uint16(pgPacketData[parameterValuesLengthStartIndex:parameterValuesLengthEndIndex])

	parameterValuesIndex := parameterValuesLengthEndIndex
	parameterValues := make([][]byte, parameterValuesLength)
	for i := 0; i < int(parameterValuesLength); i++ {
		lengthBytes := pgPacketData[parameterValuesIndex : parameterValuesIndex+4]
		if bytes.Compare(lengthBytes, []byte{255, 255, 255, 255}) == 0 {
			parameterValuesIndex += 4
			continue
		}
		valueLength := binary.BigEndian.Uint32(lengthBytes)
		parameterValueStartIndex := parameterValuesIndex + 4
		parameterValueEndIndex := parameterValueStartIndex + int(valueLength)
		parameterValues[i] = pgPacketData[parameterValueStartIndex:parameterValueEndIndex]
		parameterValuesIndex = parameterValueEndIndex
	}

	resultFormatsLengthStartIndex := parameterValuesIndex
	resultFormatsLengthEndIndex := resultFormatsLengthStartIndex + 2
	resultFormatsLength := binary.BigEndian.Uint16(pgPacketData[resultFormatsLengthStartIndex:resultFormatsLengthEndIndex])

	resultFormatsIndex := resultFormatsLengthEndIndex
	resultFormats := make([]int16, resultFormatsLength)
	for i := 0; i < int(resultFormatsLength); i++ {
		resultFormats[i] = int16(binary.BigEndian.Uint16(pgPacketData[resultFormatsIndex : resultFormatsIndex+2]))
		resultFormatsIndex = resultFormatsIndex + 2
	}

	bindMessage.Type = 42
	bindMessage.Length = int32(messageLength)
	bindMessage.Portal = portal
	bindMessage.Statement = statement
	bindMessage.ParameterFormatsLength = int16(parameterFormatsLength)
	bindMessage.ParameterFormats = parameterFormats
	bindMessage.ParameterValuesLength = int16(parameterValuesLength)
	bindMessage.ParameterValues = parameterValues
	bindMessage.ResultFormats = int16(resultFormatsLength)
	bindMessage.ResultFormatValues = resultFormats

	return resultFormatsIndex
}