package protocol

const (
	BIND     = 66
	DESCRIBE = 68
	EXECUTE  = 69
	PARSE    = 80
	SYNC     = 83
)

func IsKnownMessage(messageType byte) bool {
	return messageType == BIND || messageType == DESCRIBE ||
		messageType == EXECUTE || messageType == PARSE || messageType == SYNC
}
