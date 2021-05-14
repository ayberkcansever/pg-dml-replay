package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeReadyForQueryMessage(t *testing.T) {
	// given
	var readyForQueryComplete ReadyForQueryMessage
	readyForQueryMessageHex := "5a0000000549"
	readyForQueryDecoded, _ := hex.DecodeString(readyForQueryMessageHex)

	// when
	DecodeReadyForQueryMessage(readyForQueryDecoded, &readyForQueryComplete)

	// then
	expectedReadyForQueryCompleteMessage := ReadyForQueryMessage{
		Type:   protocol.ReadyForQuery,
		Length: int32(5),
		Status: 73,
	}
	test.AssertEquals(t, expectedReadyForQueryCompleteMessage, readyForQueryComplete)
}
