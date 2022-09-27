module controlplane/cfgApp

replace niova/go-pumicedb-lib/client => ../../pumiceDB/client

replace common/requestResponseLib => ../requestResponseLib

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace niova/go-pumicedb-lib/server => ../../pumiceDB/server

go 1.18

require github.com/satori/go.uuid v1.2.0

require (
	common/requestResponseLib v0.0.0-00010101000000-000000000000 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	golang.org/x/sys v0.0.0-20191026070338-33540a1f6037 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000 // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000 // indirect
)
