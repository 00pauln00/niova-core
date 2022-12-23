package requestResponseLib

import (
	"github.com/google/uuid"
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

type hybridTS struct {
	Major uint32
	Minor uint64
}

type LeaseStruct struct {
	Resource     uuid.UUID
	Client       uuid.UUID
	Status       int
	LeaseGranted hybridTS
	LeaseExpiry  hybridTS
}
