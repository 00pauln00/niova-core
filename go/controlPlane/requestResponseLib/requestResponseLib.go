package requestResponseLib

type KVRequest struct {
	Operation string
	Key       string
	Prefix   string
	Value     []byte
	Rncui     string
	CheckSum  [16]byte
}

type KVResponse struct {
	Status       int
	Key          string
	Value        []byte
	RangeMap     map[string]string
	ContinueRead bool
	Prefix       string
}
