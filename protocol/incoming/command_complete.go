package incoming

import (
	"com.canseverayberk/pg-dml-replay/protocol"
	"encoding/binary"
	"strings"
)

type CommandCompleteMessage struct {
	Type   byte
	Length int32
	Tag    string
}

func DecodeCommandCompleteMessage(pgPacketData []byte, commandComplete *CommandCompleteMessage) (lastIndex int) {
	var lengthData = []byte{pgPacketData[1], pgPacketData[2], pgPacketData[3], pgPacketData[4]}
	messageLength := binary.BigEndian.Uint32(lengthData)

	tagEndIndex := messageLength
	tag := string(pgPacketData[5:tagEndIndex])

	commandComplete.Type = protocol.CommandComplete
	commandComplete.Length = int32(messageLength)
	commandComplete.Tag = tag

	return lastIndex + int(tagEndIndex) + 1
}

func (commandCompleteMessage CommandCompleteMessage) IsCommitMessage() bool {
	return strings.HasPrefix(strings.ToLower(commandCompleteMessage.Tag), "commit")
}
