package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeParseCompleteMessageForInsert(t *testing.T) {
	// given
	var parseComplete ParseCompleteMessage
	parseCompleteMessageHex := "3100000004"
	parseCompleteMessageDecoded, _ := hex.DecodeString(parseCompleteMessageHex)

	// when
	DecodeParseCompleteMessage(parseCompleteMessageDecoded, &parseComplete)

	// then
	expectedParseCompleteMessage := ParseCompleteMessage{
		Type:   protocol.ParseComplete,
		Length: int32(4),
	}
	test.AssertEquals(t, expectedParseCompleteMessage, parseComplete)
}
