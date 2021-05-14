package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeBindCompleteMessageForInsert(t *testing.T) {
	// given
	var bindComplete BindCompleteMessage
	bindCompleteMessageHex := "3200000004"
	bindCompleteMessageDecoded, _ := hex.DecodeString(bindCompleteMessageHex)

	// when
	DecodeBindCompleteMessage(bindCompleteMessageDecoded, &bindComplete)

	// then
	expectedBindCompleteMessage := BindCompleteMessage{
		Type:   protocol.BindComplete,
		Length: int32(4),
	}
	test.AssertEquals(t, expectedBindCompleteMessage, bindComplete)
}
