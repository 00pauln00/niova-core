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
	UUID	[16]byte
	Cmd	string
}
