package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"testing"
)

func TestDecodeDataRowMessage(t *testing.T) {
	// given
	var dataRowMessage DataRowMessage
	dataRowMessageHex := "44000000a2000f00000007323838303432380000001a323032312d30352d30362032333a35393a34352e3437323137310000000331303000000016476c632dc59e6f6b2d53656c6c657253746f72652d33000000054f524d2d3200000013c59e6f6b205472656e64796f6c7465737431320000000747524f434552590000000133ffffffff000000013000000001310000000130000000013000000001300000000130"
	dataRowMessageDecoded, _ := hex.DecodeString(dataRowMessageHex)

	// when
	DecodeDataRowMessage(dataRowMessageDecoded, &dataRowMessage)
	// todo: assert column values
	dataRowMessage.ColumnValues = nil

	// then
	expectedDataRowMessage := DataRowMessage{
		Type:          protocol.DataRow,
		Length:        int32(162),
		ColumnCount:   15,
		ColumnLengths: []int32{7, 26, 3, 22, 5, 19, 7, 1, -1, 1, 1, 1, 1, 1, 1},
	}
	test.AssertEquals(t, expectedDataRowMessage, dataRowMessage)
}
