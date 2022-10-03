module niova/go-pumicedb-lib/server

go 1.16

require github.com/mattn/go-pointer v0.0.1

replace niova/go-pumicedb-lib/common => ../common

replace common/serfAgent => ../../serf/agent

require (
	common/serfAgent v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.9.0
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)
