module common/pmdbServer

go 1.16

replace niova/go-pumicedb-lib/server => ../../../../server

replace niova/go-pumicedb-lib/common => ../../../../common

replace common/serfAgent => ../../Serf/agent

replace common/requestResponseLib => ../../RequestResponselib

require (
	common/requestResponseLib v0.0.0-00010101000000-000000000000 // indirect
	common/serfAgent v0.0.0-00010101000000-000000000000 // indirect
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
)
