package leaseLib

import (
	uuid "github.com/satori/go.uuid"
	list "container/list"
)

const (
	GET         int = 0
	PUT             = 1
	LOOKUP          = 2
	REFRESH         = 3
	GET_VALIDATE    = 4
	LOOKUP_VALIDATE = 5
	INVALID         = 0
	INPROGRESS      = 1
	EXPIRED         = 2
	AIU             = 3
	GRANTED         = 4
	NULL            = 5
)

type LeaseReq struct {
	Rncui     string
	Client    uuid.UUID
	Resource  uuid.UUID
	Operation int
}

type LeaderTS struct {
	LeaderTerm int64
	LeaderTime int64
}

type LeaseStruct struct {
	Resource   uuid.UUID
	Client     uuid.UUID
	Status     string
	LeaseState int
	TTL        int
	TimeStamp  LeaderTS
	ListElement *list.Element
}
