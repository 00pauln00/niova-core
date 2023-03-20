module leaseClient

replace niova/go-pumicedb-lib/client => ../../../../pumiceDB/client

replace niova/go-pumicedb-lib/common => ../../../../pumiceDB/common

replace common/leaseLib => ../../../../pumiceDB/lease/common

replace LeaseLib/leaseClient => ../../../../pumiceDB/lease/client

go 1.18

require (
	LeaseLib/leaseClient v0.0.0-00010101000000-000000000000
	common/leaseLib v0.0.0-00010101000000-000000000000
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.9.0
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)

require (
	github.com/google/uuid v1.3.0 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)
