package network

import (
	"bytes"
	"com.canseverayberk/pg-dml-replay/db"
	"com.canseverayberk/pg-dml-replay/kafka"
	"com.canseverayberk/pg-dml-replay/protocol"
	"com.canseverayberk/pg-dml-replay/test"
	"encoding/hex"
	"sync"
	"testing"
)

func TestProcessMessageBES(t *testing.T) {
	// given
	bindMessageBytes, _ := hex.DecodeString("420000000f00535f3200000000000000")
	executeMessageBytes, _ := hex.DecodeString("45000000090000000001")
	syncMessageBytes, _ := hex.DecodeString("5300000004")
	var packetDataBytes bytes.Buffer
	packetDataBytes.Write(bindMessageBytes)
	packetDataBytes.Write(executeMessageBytes)
	packetDataBytes.Write(syncMessageBytes)

	// when
	processMessage(packetDataBytes.Bytes())

	// then
	expectedBindMessage := protocol.BindMessage{
		Type:               protocol.BIND,
		Length:             int32(15),
		Portal:             "",
		Statement:          "S_2",
		ParameterFormats:   []int16{},
		ParameterValues:    [][]byte{},
		ResultFormatValues: []int16{},
	}
	expectedExecuteMessage := protocol.ExecuteMessage{
		Type:             protocol.EXECUTE,
		Length:           int32(9),
		Portal:           "",
		RowCountToReturn: int32(1),
	}
	expectedSyncMessage := protocol.SyncMessage{
		Type:   protocol.SYNC,
		Length: int32(4),
	}

	bindMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedBindMessage, bindMessage)

	executeMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedExecuteMessage, executeMessage)

	syncMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedSyncMessage, syncMessage)
}

func TestProcessMessagePBDES(t *testing.T) {
	// given
	parseMessageBytes, _ := hex.DecodeString("50000000280053484f57205452414e53414354494f4e2049534f4c4154494f4e204c4556454c000000")
	bindMessageBytes, _ := hex.DecodeString("420000000c0000000000000000")
	describeMessageBytes, _ := hex.DecodeString("44000000065000")
	executeMessageBytes, _ := hex.DecodeString("45000000090000000001")
	syncMessageBytes, _ := hex.DecodeString("5300000004")
	var packetDataBytes bytes.Buffer
	packetDataBytes.Write(parseMessageBytes)
	packetDataBytes.Write(bindMessageBytes)
	packetDataBytes.Write(describeMessageBytes)
	packetDataBytes.Write(executeMessageBytes)
	packetDataBytes.Write(syncMessageBytes)

	// when
	processMessage(packetDataBytes.Bytes())

	// then
	expectedParseMessage := protocol.ParseMessage{
		Type:           protocol.PARSE,
		Length:         int32(40),
		Statement:      "",
		Query:          "SHOW TRANSACTION ISOLATION LEVEL",
		ParameterTypes: []int32{},
	}
	expectedBindMessage := protocol.BindMessage{
		Type:               protocol.BIND,
		Length:             int32(12),
		Portal:             "",
		Statement:          "",
		ParameterFormats:   []int16{},
		ParameterValues:    [][]byte{},
		ResultFormatValues: []int16{},
	}
	expectedDescribeMessage := protocol.DescribeMessage{
		Type:   protocol.DESCRIBE,
		Length: int32(6),
		Portal: "P",
	}
	expectedExecuteMessage := protocol.ExecuteMessage{
		Type:             protocol.EXECUTE,
		Length:           int32(9),
		Portal:           "",
		RowCountToReturn: int32(1),
	}
	expectedSyncMessage := protocol.SyncMessage{
		Type:   protocol.SYNC,
		Length: int32(4),
	}

	parseMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedParseMessage, parseMessage)

	bindMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedBindMessage, bindMessage)

	describeMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedDescribeMessage, describeMessage)

	executeMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedExecuteMessage, executeMessage)

	syncMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedSyncMessage, syncMessage)
}

func TestProcessMessagePBEPBDES(t *testing.T) {
	// given
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func(wg *sync.WaitGroup) {
		expectedDmlQuery := db.DmlQuery{
			Query: "UPDATE public.t2 SET c = $1 WHERE a = $2",
			Parameters: []db.QueryParameter{
				{
					Type:  0,
					Value: []byte{118, 97, 114, 99, 104, 97, 114, 50},
				},
				{
					Type:  1,
					Value: []byte{0, 0, 0, 1},
				},
			},
		}
		dmlQuery := <-kafka.DmlKafKaMessageChannel
		test.AssertEquals(t, expectedDmlQuery, dmlQuery)
		wg.Done()
	}(&wg)

	parseMessageBytes1, _ := hex.DecodeString("500000000d00424547494e000000")
	bindMessageBytes1, _ := hex.DecodeString("420000000c0000000000000000")
	executeMessageBytes1, _ := hex.DecodeString("45000000090000000000")
	parseMessageBytes2, _ := hex.DecodeString("500000003800555044415445207075626c69632e7432205345542063203d2024312057484552452061203d2024320000020000041300000017")
	bindMessageBytes2, _ := hex.DecodeString("42000000240000000200000001000200000008766172636861723200000004000000010000")
	describeMessageBytes, _ := hex.DecodeString("44000000065000")
	executeMessageBytes2, _ := hex.DecodeString("45000000090000000001")
	syncMessageBytes, _ := hex.DecodeString("5300000004")
	var packetDataBytes bytes.Buffer
	packetDataBytes.Write(parseMessageBytes1)
	packetDataBytes.Write(bindMessageBytes1)
	packetDataBytes.Write(executeMessageBytes1)
	packetDataBytes.Write(parseMessageBytes2)
	packetDataBytes.Write(bindMessageBytes2)
	packetDataBytes.Write(describeMessageBytes)
	packetDataBytes.Write(executeMessageBytes2)
	packetDataBytes.Write(syncMessageBytes)

	// when
	processMessage(packetDataBytes.Bytes())

	// then
	expectedParseMessage1 := protocol.ParseMessage{
		Type:           protocol.PARSE,
		Length:         int32(13),
		Statement:      "",
		Query:          "BEGIN",
		ParameterTypes: []int32{},
	}
	expectedBindMessage1 := protocol.BindMessage{
		Type:               protocol.BIND,
		Length:             int32(12),
		Portal:             "",
		Statement:          "",
		ParameterFormats:   []int16{},
		ParameterValues:    [][]byte{},
		ResultFormatValues: []int16{},
	}
	expectedExecuteMessage1 := protocol.ExecuteMessage{
		Type:             protocol.EXECUTE,
		Length:           int32(9),
		Portal:           "",
		RowCountToReturn: int32(0),
	}
	expectedParseMessage2 := protocol.ParseMessage{
		Type:           protocol.PARSE,
		Length:         int32(56),
		Statement:      "",
		Query:          "UPDATE public.t2 SET c = $1 WHERE a = $2",
		ParameterTypes: []int32{1043, 23},
	}
	expectedBindMessage2 := protocol.BindMessage{
		Type:             protocol.BIND,
		Length:           int32(36),
		Portal:           "",
		Statement:        "",
		ParameterFormats: []int16{0, 1},
		ParameterValues: [][]byte{
			{118, 97, 114, 99, 104, 97, 114, 50},
			{0, 0, 0, 1},
		},
		ResultFormatValues: []int16{},
	}
	expectedDescribeMessage := protocol.DescribeMessage{
		Type:   protocol.DESCRIBE,
		Length: int32(6),
		Portal: "P",
	}
	expectedExecuteMessage2 := protocol.ExecuteMessage{
		Type:             protocol.EXECUTE,
		Length:           int32(9),
		Portal:           "",
		RowCountToReturn: int32(1),
	}
	expectedSyncMessage := protocol.SyncMessage{
		Type:   protocol.SYNC,
		Length: int32(4),
	}

	parseMessage1, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedParseMessage1, parseMessage1)

	bindMessage1, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedBindMessage1, bindMessage1)

	executeMessage1, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedExecuteMessage1, executeMessage1)

	parseMessage2, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedParseMessage2, parseMessage2)

	bindMessage2, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedBindMessage2, bindMessage2)

	describeMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedDescribeMessage, describeMessage)

	executeMessage2, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedExecuteMessage2, executeMessage2)

	syncMessage, _ := messageQueue.Dequeue()
	test.AssertEquals(t, expectedSyncMessage, syncMessage)
}
