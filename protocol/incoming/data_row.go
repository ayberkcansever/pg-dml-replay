package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
	"fmt"
)

type DataRowMessage struct {
	Type          byte
	Length        int32
	ColumnCount   int16
	ColumnLengths []int32
	ColumnValues  [][]byte
}

func DecodeDataRowMessage(pgPacketData []byte, dataRowMessage *DataRowMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	var columnCountData = []byte{pgPacketData[5], pgPacketData[6]}
	columnCount := int16(binary.BigEndian.Uint16(columnCountData))

	columnLengths := make([]int32, columnCount)
	columnValues := make([][]byte, columnCount)

	lastEndIndex := 7
	for i := 0; i < int(columnCount); i++ {
		startIndex := lastEndIndex
		columnLength, endIndex := protocol.ReadInt32(pgPacketData, startIndex)
		if columnLength == -1 {
			columnLengths[i] = columnLength
			columnValues[i] = nil
			lastEndIndex = endIndex
			continue
		}
		if columnLength > 100000 {
			fmt.Println("aaaaa")
		}
		columnValue := pgPacketData[endIndex:(endIndex + int(columnLength))]
		columnLengths[i] = columnLength
		columnValues[i] = columnValue
		lastEndIndex = endIndex + int(columnLength)
	}

	dataRowMessage.Type = protocol.DataRow
	dataRowMessage.Length = int32(messageLength)
	dataRowMessage.ColumnCount = columnCount
	dataRowMessage.ColumnLengths = columnLengths
	dataRowMessage.ColumnValues = columnValues

	return lastEndIndex
}
