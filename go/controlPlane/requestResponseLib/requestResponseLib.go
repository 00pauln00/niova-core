package requestResponseLib

import uuid "github.com/satori/go.uuid"

const (
	APP_REQ     int = 0
	LEASE_REQ       = 1
	LOOKOUT_REQ     = 2
)

type AppRequest struct {
	Rncui          string
	RequestType    int
	RequestPayload []byte
	KvOperation    string
	LeaseOperation int
	Client         uuid.UUID
	Resource       uuid.UUID
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
	Operation  string
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
