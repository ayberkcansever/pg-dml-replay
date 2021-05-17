package protocol

const (
	ParseComplete      = 49
	BindComplete       = 50
	Bind               = 66
	CommandComplete    = 67
	Describe           = 68
	Execute            = 69
	EmptyQueryResponse = 73
	Parse              = 80
	Sync               = 83
	ReadyForQuery      = 90
	NoData             = 110
)

func IsKnownOutgoingMessage(messageType byte) bool {
	return messageType == Bind || messageType == Describe ||
		messageType == Execute || messageType == Parse || messageType == Sync
}

func IsKnownIncomingMessage(messageType byte) bool {
	return messageType == BindComplete || messageType == ParseComplete ||
		messageType == CommandComplete || messageType == NoData ||
		messageType == EmptyQueryResponse || messageType == ReadyForQuery
}
