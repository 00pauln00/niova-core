module common/pmdbServer

go 1.16

replace niova/go-pumicedb-lib/server => ../../../../pumiceDB/server

replace niova/go-pumicedb-lib/common => ../../../../pumiceDB/common

replace niova/go-pumicedb-lib/client => ../../../../pumiceDB/client

replace common/serfAgent => ../../../serf/agent

replace LeaseLib/leaseServer => ../../../../pumiceDB/lease/server

replace common/leaseLib => ../../../../pumiceDB/lease/common

require (
	LeaseLib/leaseServer v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.9.0
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
)
