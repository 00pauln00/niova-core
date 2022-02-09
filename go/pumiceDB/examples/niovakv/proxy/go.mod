module controlplane/proxy

replace common/httpServer => ../../../../HTTP/server

replace common/requestResponseLib => ../../common/RequestResponselib

replace niova/go-pumicedb-lib/client => ../../../client

replace niova/go-pumicedb-lib/common => ../../../common

replace common/serfAgent => ../../../../Serf/agent

go 1.16

require (
	common/httpServer v0.0.0-00010101000000-000000000000
	common/serfAgent v0.0.0-00010101000000-000000000000
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000 // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)