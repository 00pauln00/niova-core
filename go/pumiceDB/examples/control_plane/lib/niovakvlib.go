package niovakvlib

type NiovaKV struct {
	InputOps   string
	InputKey   string
	InputValue []byte
	CheckSum   [16]byte
}

type NiovaKVResponse struct {
	RespStatus int
	RespValue  []byte
}
