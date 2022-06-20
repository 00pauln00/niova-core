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

type LookoutRequest struct {
	NISD	[16]byte
	Cmd	string
}
