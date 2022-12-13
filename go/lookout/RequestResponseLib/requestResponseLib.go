package requestResponseLib

type KVRequest struct {
	Operation  string
	Key        string
	Value      []byte
	CheckSum   [16]byte
}

type KVResponse struct {
	Status int
	Value  []byte
}

type PMDBKVResponse struct {
	Status       int
	Key          string
	ResultMap    map[string][]byte
	ContinueRead bool
	Prefix       string
	SeqNum	     uint64
	SnapMiss     bool
}

type LookoutRequest struct {
	UUID	[16]byte
	Cmd	string
}
