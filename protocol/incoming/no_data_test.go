package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeNoDataMessage(t *testing.T) {
	// given
	var noDataMessage NoDataMessage
	noDataMessageHex := "6e00000004"
	noDataMessageDecoded, _ := hex.DecodeString(noDataMessageHex)

	// when
	DecodeNoDataMessage(noDataMessageDecoded, &noDataMessage)

	// then
	expectedNoDataMessage := NoDataMessage{
		Type:   protocol.NoData,
		Length: int32(4),
	}
	test.AssertEquals(t, expectedNoDataMessage, noDataMessage)
}
