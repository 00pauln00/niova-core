package requestResponseLib

type KVRequest struct {
	Operation string
	Key       string
	LastKey   string
	Value     []byte
	Rncui     string
	CheckSum  [16]byte
}

type KVResponse struct {
	Status		int
	Value		[]byte
	RangeMap	map[string]string
	ContinueRead	bool
	LastKey		string
}
