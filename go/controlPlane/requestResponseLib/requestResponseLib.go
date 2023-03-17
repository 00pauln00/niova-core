package requestResponseLib

const (
	KV_WRITE      int = 0
	KV_READ           = 1
	KV_RANGE_READ     = 2
)

type KVRequest struct {
	Operation  int
	Key        string
	Prefix     string
	Value      []byte
	Rncui      string
	CheckSum   [16]byte
	SeqNum     uint64
	Consistent bool
}

type KVResponse struct {
	Status       int
	Key          string
	ResultMap    map[string][]byte
	ContinueRead bool
	Prefix       string
	SeqNum       uint64
	SnapMiss     bool
}

type LookoutRequest struct {
	UUID [16]byte
	Cmd  string
}
