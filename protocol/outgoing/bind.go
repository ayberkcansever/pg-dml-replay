package outgoing

import (
	"bytes"
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
)

type BindMessage struct {
	Type               byte
	Length             int32
	Portal             string
	Statement          string
	ParameterFormats   []int16
	ParameterValues    [][]byte
	ResultFormatValues []int16
}

func (m BindMessage) IsPreparedStatement() bool {
	return m.Statement != ""
}

func DecodeBindMessage(pgPacketData []byte, bindMessage *BindMessage) (lastIndex int) {
	messageLength := protocol.GetMessageLength(pgPacketData)
	portal, portalEndIndex := protocol.ReadUntil(pgPacketData, 5)
	statement, statementEndIndex := protocol.ReadUntil(pgPacketData, portalEndIndex)
	parameterFormatsLength, parameterFormatsLengthEndIndex := protocol.ReadInt16(pgPacketData, statementEndIndex)
	parameterFormats, parameterFormatsEndIndex := protocol.ReadInt16Array(pgPacketData, parameterFormatsLengthEndIndex, int(parameterFormatsLength))
	parameterValuesLength, parameterValuesLengthEndIndex := protocol.ReadInt16(pgPacketData, parameterFormatsEndIndex)

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

	resultFormatsLength, resultFormatsLengthEndIndex := protocol.ReadInt16(pgPacketData, parameterValuesIndex)
	resultFormats, resultFormatsIndex := protocol.ReadInt16Array(pgPacketData, resultFormatsLengthEndIndex, int(resultFormatsLength))

	bindMessage.Type = protocol.Bind
	bindMessage.Length = int32(messageLength)
	bindMessage.Portal = string(portal)
	bindMessage.Statement = string(statement)
	bindMessage.ParameterFormats = parameterFormats
	bindMessage.ParameterValues = parameterValues
	bindMessage.ResultFormatValues = resultFormats

	return resultFormatsIndex
}
