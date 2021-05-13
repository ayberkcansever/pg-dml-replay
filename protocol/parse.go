package protocol

import (
	"encoding/binary"
	"strings"
)

type ParseMessage struct {
	Type           byte
	Length         int32
	Statement      string
	Query          string
	ParameterTypes []int32
}

func (m ParseMessage) IsDMLQuery() bool {
	return strings.HasPrefix(strings.ToLower(m.Query), "insert") ||
		strings.HasPrefix(strings.ToLower(m.Query), "update") ||
		strings.HasPrefix(strings.ToLower(m.Query), "delete")
}

func DecodeParseMessage(pgPacketData []byte, parseMessage *ParseMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	statementStartIndex := 5
	statementEndIndex := statementStartIndex
	for {
		if pgPacketData[statementEndIndex] == 0 {
			statementEndIndex++
			break
		}
		statementEndIndex++
	}
	statement := string(pgPacketData[statementStartIndex : statementEndIndex-1])

	queryStartIndex := statementEndIndex
	queryEndIndex := queryStartIndex
	for {
		if pgPacketData[queryEndIndex] == 0 {
			break
		}
		queryEndIndex++
	}

	query := string(pgPacketData[queryStartIndex:queryEndIndex])

	/*parameterLengthStartIndex := queryEndIndex + 1
	parameterLengthEndIndex := parameterLengthStartIndex + 2
	parameterLength := binary.BigEndian.Uint16(pgPacketData[parameterLengthStartIndex:parameterLengthEndIndex])*/

	parameterLength, parameterLengthEndIndex := ReadInt16(pgPacketData, queryEndIndex+1)

	/*	parameterTypesIndex := parameterLengthEndIndex
		parameterTypes := make([]int32, parameterLength)
		for i := 0; i < int(parameterLength); i++ {
			parameterTypes[i] = int32(binary.BigEndian.Uint32(pgPacketData[parameterTypesIndex : parameterTypesIndex+4]))
			parameterTypesIndex = parameterTypesIndex + 4
		}*/

	parameterTypes, parameterTypesIndex := ReadInt32Array(pgPacketData, parameterLengthEndIndex, int(parameterLength))

	parseMessage.Type = 50
	parseMessage.Length = int32(messageLength)
	parseMessage.Statement = statement
	parseMessage.Query = query
	parseMessage.ParameterTypes = parameterTypes

	return parameterTypesIndex
}
