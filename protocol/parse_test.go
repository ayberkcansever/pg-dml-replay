package protocol

import (
	"encoding/hex"
	"reflect"
	"testing"
)

func TestDecodeParseMessageWithEmptyQuery(t *testing.T) {
	// given
	var parseMessage ParseMessage
	parseMessageHex := "420000000c0000000000000000"
	parseMessageDecoded, _ := hex.DecodeString(parseMessageHex)

	// when
	DecodeParseMessage(parseMessageDecoded, &parseMessage)

	// then
	expectedParseMessage := ParseMessage{
		Type:           PARSE,
		Length:         int32(12),
		Statement:      "",
		Query:          "",
		ParameterTypes: []int32{},
	}
	if !reflect.DeepEqual(expectedParseMessage, parseMessage) {
		t.Errorf("expected %q, got %q", expectedParseMessage, parseMessage)
	}
}

func TestDecodeParseMessageWithBeginQuery(t *testing.T) {
	// given
	var parseMessage ParseMessage
	parseMessageHex := "500000000d00424547494e000000"
	parseMessageDecoded, _ := hex.DecodeString(parseMessageHex)

	// when
	DecodeParseMessage(parseMessageDecoded, &parseMessage)

	// then
	expectedParseMessage := ParseMessage{
		Type:           PARSE,
		Length:         int32(13),
		Statement:      "",
		Query:          "BEGIN",
		ParameterTypes: []int32{},
	}
	if !reflect.DeepEqual(expectedParseMessage, parseMessage) {
		t.Errorf("expected %q, got %q", expectedParseMessage, parseMessage)
	}
}

func TestDecodeParseMessageWithStatement(t *testing.T) {
	// given
	var parseMessage ParseMessage
	parseMessageHex := "500000000b535f3200000000"
	parseMessageDecoded, _ := hex.DecodeString(parseMessageHex)

	// when
	DecodeParseMessage(parseMessageDecoded, &parseMessage)

	// then
	expectedParseMessage := ParseMessage{
		Type:           PARSE,
		Length:         int32(11),
		Statement:      "S_2",
		Query:          "",
		ParameterTypes: []int32{},
	}
	if !reflect.DeepEqual(expectedParseMessage, parseMessage) {
		t.Errorf("expected %q, got %q", expectedParseMessage, parseMessage)
	}
}

func TestDecodeParseMessageWithParameters(t *testing.T) {
	// given
	var parseMessage ParseMessage
	parseMessageHex := "500000003800555044415445207075626c69632e7431205345542062203d2024312057484552452061203d2024320000020000001400000017"
	parseMessageDecoded, _ := hex.DecodeString(parseMessageHex)

	// when
	DecodeParseMessage(parseMessageDecoded, &parseMessage)

	// then
	expectedParseMessage := ParseMessage{
		Type:           PARSE,
		Length:         int32(56),
		Statement:      "",
		Query:          "UPDATE public.t1 SET b = $1 WHERE a = $2",
		ParameterTypes: []int32{20, 23},
	}
	if !reflect.DeepEqual(expectedParseMessage, parseMessage) {
		t.Errorf("expected %q, got %q", expectedParseMessage, parseMessage)
	}
}
