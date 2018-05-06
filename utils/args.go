package utils

// IOReadArgs - represents structure which passed via rpc
type IOReadArgs struct {
	Filename *string
	Offset   int32
	Count    int32
}

// IOWriteArgs - represents structure which passed via rpc
type IOWriteArgs struct {
	Filename *string
	Offset   int32
	Data     *[]byte
}
