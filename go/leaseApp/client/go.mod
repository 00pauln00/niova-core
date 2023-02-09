module leaseClient

replace niova/go-pumicedb-lib/client => ../../pumiceDB/client

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace common/requestResponseLib => ../requestResponseLib

replace common/leaseLib => ../../pumiceDB/lease/common

go 1.18

require (
	common/leaseLib v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.3.0
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)

require golang.org/x/sys v0.0.0-20191026070338-33540a1f6037 // indirect
