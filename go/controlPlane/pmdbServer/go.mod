module common/pmdbServer

go 1.16

replace niova/go-pumicedb-lib/server => ../../pumiceDB/server

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace common/serfAgent => ../../serf/agent

replace common/requestResponseLib => ../requestResponseLib

replace common/specificCompressionLib => ../../specificCompressionLib

replace common/lookout => ../../lookout/ctlMonitor

replace common/prometheus_handler => ../../lookout/prometheusHandler

require (
	common/lookout v0.0.0-00010101000000-000000000000
	common/requestResponseLib v0.0.0-00010101000000-000000000000
	common/serfAgent v0.0.0-00010101000000-000000000000
	common/specificCompressionLib v0.0.0-00010101000000-000000000000
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.9.0
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
)
