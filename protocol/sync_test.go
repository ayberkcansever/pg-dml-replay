package protocol

import (
	"encoding/hex"
	"reflect"
	"testing"
)

func TestDecodeSyncMessage(t *testing.T) {
	// given
	var syncMessage SyncMessage
	syncMessageHex := "5300000004"
	syncMessageDecoded, _ := hex.DecodeString(syncMessageHex)

	// when
	DecodeSyncMessage(syncMessageDecoded, &syncMessage)

	// then
	expectedSyncMessage := SyncMessage{
		Type:   SYNC,
		Length: int32(4),
	}
	if !reflect.DeepEqual(expectedSyncMessage, syncMessage) {
		t.Errorf("expected %q, got %q", expectedSyncMessage, syncMessage)
	}
}
