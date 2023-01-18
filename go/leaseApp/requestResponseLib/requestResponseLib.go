package requestResponseLib

import (
	"github.com/google/uuid"
)

const (
	GET     int = 0
	PUT         = 1
	LOOKUP      = 2
	REFRESH     = 3
	GRANTED     = 0
	INPROGRESS  = 1
	EXPIRED     = 2
	AIU	    = 3
	INVALID     = 4
)

type LeaseReq struct {
	Client    uuid.UUID
	Resource  uuid.UUID
	Operation int
}

type LeaseResp struct {
	Client    uuid.UUID
	Resource  uuid.UUID
	Status    string
	State     string
	Timestamp string
}

type LeaderTS struct {
        LeaderTerm    int64
        LeaderTime    int64
}

type LeaseStruct struct {
	Resource   uuid.UUID
	Client     uuid.UUID
	Status     int
	LeaseState int
	TTL        int
	TimeStamp  LeaderTS
}
