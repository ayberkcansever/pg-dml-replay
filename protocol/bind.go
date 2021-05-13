package protocol

import (
	"bytes"
	"encoding/binary"
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

func (m BindMessage) IsPreparedStatement() bool {
	return m.Statement != ""
}

func DecodeBindMessage(pgPacketData []byte, bindMessage *BindMessage) (lastIndex int) {
	messageLength := GetMessageLength(pgPacketData)
	portal, portalEndIndex := ReadUntil(pgPacketData, 5)
	statement, statementEndIndex := ReadUntil(pgPacketData, portalEndIndex)
	parameterFormatsLength, parameterFormatsLengthEndIndex := ReadInt16(pgPacketData, statementEndIndex)
	parameterFormats, parameterFormatsEndIndex := ReadInt16Array(pgPacketData, parameterFormatsLengthEndIndex, int(parameterFormatsLength))
	parameterValuesLength, parameterValuesLengthEndIndex := ReadInt16(pgPacketData, parameterFormatsEndIndex)

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

	resultFormatsLength, resultFormatsLengthEndIndex := ReadInt16(pgPacketData, parameterValuesIndex)
	resultFormats, resultFormatsIndex := ReadInt16Array(pgPacketData, resultFormatsLengthEndIndex, int(resultFormatsLength))

	bindMessage.Type = 42
	bindMessage.Length = int32(messageLength)
	bindMessage.Portal = string(portal)
	bindMessage.Statement = string(statement)
	bindMessage.ParameterFormatsLength = int16(parameterFormatsLength)
	bindMessage.ParameterFormats = parameterFormats
	bindMessage.ParameterValuesLength = int16(parameterValuesLength)
	bindMessage.ParameterValues = parameterValues
	bindMessage.ResultFormats = int16(resultFormatsLength)
	bindMessage.ResultFormatValues = resultFormats

	return resultFormatsIndex
}
