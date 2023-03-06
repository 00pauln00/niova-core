package requestResponseLib

const (
	APP_REQ     int = 0
	LEASE_REQ       = 1
	LOOKOUT_REQ     = 2
)

const (
	KV_WRITE      int = 0
	KV_READ           = 1
	KV_RANGE_READ     = 2
)

type AppRequest struct {
	Rncui          string
	RequestType    int
	RequestPayload []byte
	Operation      int
	Key            string
	Prefix         string
	Value          []byte
	CheckSum       [16]byte
	SeqNum         uint64
	Consistent     bool
	ReqType        int
	UUID           [16]byte
	Cmd            string
}

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
