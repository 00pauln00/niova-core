package requestResponseLib

type KVRequest struct {
	Operation string
	Key       string
	Prefix   string
	Value     []byte
	Rncui     string
	CheckSum  [16]byte
	SeqNum	  uint64
}

type KVResponse struct {
	Status       int
	Key          string
	Value        []byte
	ResultMap     map[string]string
	ContinueRead bool
	Prefix       string
	SeqNum	     uint64
}
