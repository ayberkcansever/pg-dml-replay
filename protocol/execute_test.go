package protocol

import (
	"encoding/hex"
	"reflect"
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
		Type:             EXECUTE,
		Length:           int32(9),
		Portal:           "",
		RowCountToReturn: int32(0),
	}
	if !reflect.DeepEqual(expectedExecuteMessage, executeMessage) {
		t.Errorf("expected %q, got %q", expectedExecuteMessage, executeMessage)
	}
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
		Type:             EXECUTE,
		Length:           int32(9),
		Portal:           "",
		RowCountToReturn: int32(1),
	}
	if !reflect.DeepEqual(expectedExecuteMessage, executeMessage) {
		t.Errorf("expected %q, got %q", expectedExecuteMessage, executeMessage)
	}
}
