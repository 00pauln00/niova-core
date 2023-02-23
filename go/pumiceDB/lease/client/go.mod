module LeaseLib/LeaseClient

replace common/leaseLib => ../common

replace niova/go-pumicedb-lib/client => ../../client

replace niova/go-pumicedb-lib/common => ../../common

go 1.18

require (
	common/leaseLib v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.3.0
	github.com/sirupsen/logrus v1.9.0
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
)

require (
	github.com/satori/go.uuid v1.2.0 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000 // indirect
)
