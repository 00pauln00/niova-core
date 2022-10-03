module niova/go-pumicedb-lib/common

replace common/serfAgent => ../../serf/agent

go 1.16

require (
	common/serfAgent v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.9.0
)
