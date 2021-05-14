package outgoing

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeExecuteMessage(t *testing.T) {
	// given
	var executeMessage ExecuteMessage
	executeMessageHex := "45000000090000000000"
	executeMessageDecoded, _ := hex.DecodeString(executeMessageHex)

	// when
	DecodeExecuteMessage(executeMessageDecoded, &executeMessage)

	// then
	expectedExecuteMessage := ExecuteMessage{
		Type:             protocol.Execute,
		Length:           int32(9),
		Portal:           "",
		RowCountToReturn: int32(0),
	}
	test.AssertEquals(t, expectedExecuteMessage, executeMessage)
}

func TestDecodeExecuteMessageReturns1Row(t *testing.T) {
	// given
	var executeMessage ExecuteMessage
	executeMessageHex := "45000000090000000001"
	executeMessageDecoded, _ := hex.DecodeString(executeMessageHex)

	// when
	DecodeExecuteMessage(executeMessageDecoded, &executeMessage)

	// then
	expectedExecuteMessage := ExecuteMessage{
		Type:             protocol.Execute,
		Length:           int32(9),
		Portal:           "",
		RowCountToReturn: int32(1),
	}
	test.AssertEquals(t, expectedExecuteMessage, executeMessage)
}
