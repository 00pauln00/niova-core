package niovakvlib

type NiovaKV struct {
	InputOps   string
	InputKey   string
	InputValue []byte
}

type NiovaKVResponse struct {
	RespStatus int
	RespValue  []byte
}
