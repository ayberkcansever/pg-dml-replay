package protocol

import (
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeBindMessage(t *testing.T) {
	// given
	var bindMessage BindMessage
	bindMessageHex := "420000000c0000000000000000"
	bindMessageDecoded, _ := hex.DecodeString(bindMessageHex)

	// when
	DecodeBindMessage(bindMessageDecoded, &bindMessage)

	// then
	expectedBindMessage := BindMessage{
		Type:               BIND,
		Length:             int32(12),
		Portal:             "",
		Statement:          "",
		ParameterFormats:   []int16{},
		ParameterValues:    [][]byte{},
		ResultFormatValues: []int16{},
	}
	test.AssertEquals(t, expectedBindMessage, bindMessage)
}

func TestDecodeBindMessageWithParameters1(t *testing.T) {
	// given
	var bindMessage BindMessage
	bindMessageHex := "42000000240000000200010001000200000008000000000000115c00000004000000060000"
	bindMessageDecoded, _ := hex.DecodeString(bindMessageHex)

	// when
	DecodeBindMessage(bindMessageDecoded, &bindMessage)

	// then
	expectedBindMessage := BindMessage{
		Type:               BIND,
		Length:             int32(36),
		Portal:             "",
		Statement:          "",
		ParameterFormats:   []int16{1, 1},
		ParameterValues:    [][]byte{{0, 0, 0, 0, 0, 0, 17, 92}, {0, 0, 0, 6}},
		ResultFormatValues: []int16{},
	}
	test.AssertEquals(t, expectedBindMessage, bindMessage)
}

func TestDecodeBindMessageWithParameters2(t *testing.T) {
	// given
	var bindMessage BindMessage
	bindMessageHex := "420000005c00000005000100010000000000000005000000080000000000000001000000080000000000000004000000077661726368617200000016323032312d30352d31342030303a30303a34372b30300000000531302e39390000"
	bindMessageDecoded, _ := hex.DecodeString(bindMessageHex)

	// when
	DecodeBindMessage(bindMessageDecoded, &bindMessage)

	// then
	expectedBindMessage := BindMessage{
		Type:             BIND,
		Length:           int32(92),
		Portal:           "",
		Statement:        "",
		ParameterFormats: []int16{1, 1, 0, 0, 0},
		ParameterValues: [][]byte{
			{0, 0, 0, 0, 0, 0, 0, 1},
			{0, 0, 0, 0, 0, 0, 0, 4},
			{118, 97, 114, 99, 104, 97, 114},
			{50, 48, 50, 49, 45, 48, 53, 45, 49, 52, 32, 48, 48, 58, 48, 48, 58, 52, 55, 43, 48, 48},
			{49, 48, 46, 57, 57},
		},
		ResultFormatValues: []int16{},
	}
	test.AssertEquals(t, expectedBindMessage, bindMessage)
}
