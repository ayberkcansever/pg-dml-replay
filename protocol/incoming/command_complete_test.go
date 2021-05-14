package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeCommandCompleteMessageForInsert(t *testing.T) {
	// given
	var commandComplete CommandCompleteMessage
	commandCompleteMessageHex := "430000000f494e534552542030203100"
	commandCompleteMessageDecoded, _ := hex.DecodeString(commandCompleteMessageHex)

	// when
	DecodeCommandCompleteMessage(commandCompleteMessageDecoded, &commandComplete)

	// then
	expectedCommandCompleteMessage := CommandCompleteMessage{
		Type:   protocol.CommandComplete,
		Length: int32(15),
		Tag:    "INSERT 0 1",
	}
	test.AssertEquals(t, expectedCommandCompleteMessage, commandComplete)
}

func TestDecodeCommandCompleteMessageForUpdate(t *testing.T) {
	// given
	var commandComplete CommandCompleteMessage
	commandCompleteMessageHex := "430000000d555044415445203100"
	commandCompleteMessageDecoded, _ := hex.DecodeString(commandCompleteMessageHex)

	// when
	DecodeCommandCompleteMessage(commandCompleteMessageDecoded, &commandComplete)

	// then
	expectedCommandCompleteMessage := CommandCompleteMessage{
		Type:   protocol.CommandComplete,
		Length: int32(13),
		Tag:    "UPDATE 1",
	}
	test.AssertEquals(t, expectedCommandCompleteMessage, commandComplete)
}

func TestDecodeCommandCompleteMessageForDelete(t *testing.T) {
	// given
	var commandComplete CommandCompleteMessage
	commandCompleteMessageHex := "430000000d44454c455445203100"
	commandCompleteMessageDecoded, _ := hex.DecodeString(commandCompleteMessageHex)

	// when
	DecodeCommandCompleteMessage(commandCompleteMessageDecoded, &commandComplete)

	// then
	expectedCommandCompleteMessage := CommandCompleteMessage{
		Type:   protocol.CommandComplete,
		Length: int32(13),
		Tag:    "DELETE 1",
	}
	test.AssertEquals(t, expectedCommandCompleteMessage, commandComplete)
}

func TestDecodeCommandCompleteMessageForBegin(t *testing.T) {
	// given
	var commandComplete CommandCompleteMessage
	commandCompleteMessageHex := "430000000a424547494e00"
	commandCompleteMessageDecoded, _ := hex.DecodeString(commandCompleteMessageHex)

	// when
	DecodeCommandCompleteMessage(commandCompleteMessageDecoded, &commandComplete)

	// then
	expectedCommandCompleteMessage := CommandCompleteMessage{
		Type:   protocol.CommandComplete,
		Length: int32(10),
		Tag:    "BEGIN",
	}
	test.AssertEquals(t, expectedCommandCompleteMessage, commandComplete)
}
