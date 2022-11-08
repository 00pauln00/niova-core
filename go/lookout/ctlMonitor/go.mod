module controlplane/lookout

replace common/requestResponseLib => ../RequestResponseLib/

replace common/prometheus_handler => ../prometheusHandler/

replace common/serfAgent => ../../serf/agent

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

go 1.16

require (
	github.com/fsnotify/fsnotify v1.5.1
	github.com/google/uuid v1.3.0
)

require (
	common/prometheus_handler v0.0.0-00010101000000-000000000000
	common/requestResponseLib v0.0.0-00010101000000-000000000000
	common/serfAgent v0.0.0-00010101000000-000000000000
	github.com/hashicorp/serf v0.9.6 // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)
