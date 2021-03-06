package outgoing

import (
	"com.canseverayberk/pg-dml-replay/protocol"
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

	parameterLength, parameterLengthEndIndex := protocol.ReadInt16(pgPacketData, queryEndIndex+1)
	parameterTypes, parameterTypesIndex := protocol.ReadInt32Array(pgPacketData, parameterLengthEndIndex, int(parameterLength))

	parseMessage.Type = protocol.Parse
	parseMessage.Length = int32(messageLength)
	parseMessage.Statement = statement
	parseMessage.Query = query
	parseMessage.ParameterTypes = parameterTypes

	return parameterTypesIndex
}
