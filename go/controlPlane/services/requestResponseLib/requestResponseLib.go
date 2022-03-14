package requestResponseLib

type KVRequest struct {
	Operation string
	Key       string
	Value     []byte
	Rncui     string
	CheckSum  [16]byte
}

type KVResponse struct {
	Status int
	Value  []byte
}
