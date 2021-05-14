package outgoing

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
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
		Type:   protocol.Sync,
		Length: int32(4),
	}
	test.AssertEquals(t, expectedSyncMessage, syncMessage)
}
