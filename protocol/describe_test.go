package protocol

import (
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeDescribeMessage(t *testing.T) {
	// given
	var describeMessage DescribeMessage
	describeMessageHex := "44000000065000"
	describeMessageDecoded, _ := hex.DecodeString(describeMessageHex)

	// when
	DecodeDescribeMessage(describeMessageDecoded, &describeMessage)

	// then
	expectedDescribeMessage := DescribeMessage{
		Type:   DESCRIBE,
		Length: int32(6),
		Portal: "P",
	}
	test.AssertEquals(t, expectedDescribeMessage, describeMessage)
}
