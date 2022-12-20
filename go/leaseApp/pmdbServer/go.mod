module common/pmdbServer

go 1.16

replace niova/go-pumicedb-lib/server => ../../pumiceDB/server

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace common/serfAgent => ../../../../serf/agent

replace common/requestResponseLib => ../requestResponseLib

require (
	common/requestResponseLib v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.3.0 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.4.0 // indirect
	golang.org/x/sys v0.0.0-20200223170610-d5e6a3e2c0ae // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
)
