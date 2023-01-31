module common/pmdbServer

go 1.16

replace niova/go-pumicedb-lib/server => ../../pumiceDB/server

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace common/serfAgent => ../../serf/agent

replace common/requestResponseLib => ../requestResponseLib

replace LeaseLib/leaseServer => ../../pumiceDB/lease/server

replace common/leaseLib => ../../pumiceDB/lease/common

require (
	LeaseLib/leaseServer v0.0.0-00010101000000-000000000000
	common/leaseLib v0.0.0-00010101000000-000000000000
	common/requestResponseLib v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.3.0
	github.com/sirupsen/logrus v1.9.0
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
)
