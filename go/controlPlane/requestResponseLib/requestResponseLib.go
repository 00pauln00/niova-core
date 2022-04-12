package requestResponseLib

type KVRequest struct {
	Operation  string
	RangeQuery bool
	Key        string
	LastKeyRead string
	Value      []byte
	Rncui      string
	CheckSum   [16]byte
}

type KVResponse struct {
	Status int
	Value map[string]string 
	LastKeyRead string
}
