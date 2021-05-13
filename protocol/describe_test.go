package protocol

import (
	"encoding/hex"
	"reflect"
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
	if !reflect.DeepEqual(expectedDescribeMessage, describeMessage) {
		t.Errorf("expected %q, got %q", expectedDescribeMessage, describeMessage)
	}
}
