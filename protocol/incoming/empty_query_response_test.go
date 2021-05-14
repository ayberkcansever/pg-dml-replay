package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeEmptyQueryResponse(t *testing.T) {
	// given
	var emptyQueryResponseMessage EmptyQueryResponseMessage
	emptyQueryResponseMessageHex := "4900000004"
	emptyQueryResponseMessageDecoded, _ := hex.DecodeString(emptyQueryResponseMessageHex)

	// when
	DecodeEmptyQueryResponse(emptyQueryResponseMessageDecoded, &emptyQueryResponseMessage)

	// then
	expectedEmptyQueryResponseMessage := EmptyQueryResponseMessage{
		Type:   protocol.EmptyQueryResponse,
		Length: int32(4),
	}
	test.AssertEquals(t, expectedEmptyQueryResponseMessage, emptyQueryResponseMessage)
}
