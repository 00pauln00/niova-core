module controlplane/proxy

replace common/httpServer => ../../http/server

replace common/httpClient => ../../http/client

replace niova/go-pumicedb-lib/client => ../../pumiceDB/client

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace common/serfAgent => ../../serf/agent

replace common/requestResponseLib => ../requestResponseLib

replace common/specificCompressionLib => ../../specificCompressionLib

go 1.16

require (
	common/httpClient v0.0.0-00010101000000-000000000000
	common/httpServer v0.0.0-00010101000000-000000000000
	common/requestResponseLib v0.0.0-00010101000000-000000000000
	common/serfAgent v0.0.0-00010101000000-000000000000
	common/specificCompressionLib v0.0.0-00010101000000-000000000000
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)
