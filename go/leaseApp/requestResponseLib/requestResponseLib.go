package requestResponseLib

import (
	"github.com/google/uuid"
)


const (
	GET     int = 0
	PUT               = 1
	LOOKUP            = 2
	REFRESH           = 3
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

type LeaseStruct struct {
	Resource       uuid.UUID
	Client         uuid.UUID
	Status         int
	LeaseGrantedTS float64
	LeaseExpiryTS  float64
}
