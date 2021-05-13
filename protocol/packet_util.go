package protocol

import "encoding/binary"

func GetMessageLength(packetData []byte) int {
	var lengthData = []byte{packetData[1], packetData[2], packetData[3], packetData[4]}
	return int(binary.BigEndian.Uint32(lengthData))
}

func ReadUntil(packetData []byte, startIndex int) ([]byte, int) {
	endIndex := startIndex
	for {
		if packetData[endIndex] == 0 {
			endIndex++
			break
		}
		endIndex++
	}
	return packetData[startIndex : endIndex-1], endIndex
}

func ReadInt16(packetData []byte, startIndex int) (uint16, int) {
	endIndex := startIndex + 2
	return binary.BigEndian.Uint16(packetData[startIndex:endIndex]), endIndex
}

func ReadInt16Array(packetData []byte, startIndex int, length int) ([]int16, int) {
	parameterFormats := make([]int16, length)
	for i := 0; i < length; i++ {
		value, lastIndex := ReadInt16(packetData, startIndex)
		parameterFormats[i] = int16(value)
		startIndex = lastIndex
	}
	return parameterFormats, startIndex
}

func ReadInt32(packetData []byte, startIndex int) (uint32, int) {
	endIndex := startIndex + 4
	return binary.BigEndian.Uint32(packetData[startIndex:endIndex]), endIndex
}

func ReadInt32Array(packetData []byte, startIndex int, length int) ([]int32, int) {
	parameterFormats := make([]int32, length)
	for i := 0; i < length; i++ {
		value, lastIndex := ReadInt32(packetData, startIndex)
		parameterFormats[i] = int32(value)
		startIndex = lastIndex
	}
	return parameterFormats, startIndex
}
