package requestResponseLib

import "github.com/google/uuid"

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
