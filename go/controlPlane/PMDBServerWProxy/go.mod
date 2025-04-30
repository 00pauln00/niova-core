module controlplane/PMDBServerWProxy

replace controlplane/PMDBServer => ../pmdbServer

replace controlplane/Proxy => ../proxy

replace niova/go-pumicedb-lib/server => ../../pumiceDB/server

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace niova/go-pumicedb-lib/client => ../../pumiceDB/client

replace common/serfAgent => ../../serf/agent

replace common/httpClient => ../../http/client

replace common/httpServer => ../../http/server

replace common/requestResponseLib => ../requestResponseLib

replace common/specificCompressionLib => ../../specificCompressionLib

replace common/lookout => ../../lookout/ctlMonitor

replace LeaseLib/leaseServer => ../../pumiceDB/lease/server

replace common/leaseLib => ../../pumiceDB/lease/common

replace common/prometheus_handler => ../../lookout/prometheusHandler

go 1.22.2

require (
	controlplane/PMDBServer v0.0.0-00010101000000-000000000000
	controlplane/Proxy v0.0.0-00010101000000-000000000000
)

require (
	LeaseLib/leaseServer v0.0.0-00010101000000-000000000000 // indirect
	common/httpClient v0.0.0-00010101000000-000000000000 // indirect
	common/httpServer v0.0.0-00010101000000-000000000000 // indirect
	common/leaseLib v0.0.0-00010101000000-000000000000 // indirect
	common/lookout v0.0.0-00010101000000-000000000000 // indirect
	common/prometheus_handler v0.0.0-00010101000000-000000000000 // indirect
	common/requestResponseLib v0.0.0-00010101000000-000000000000 // indirect
	common/serfAgent v0.0.0-00010101000000-000000000000 // indirect
	common/specificCompressionLib v0.0.0-00010101000000-000000000000 // indirect
	github.com/armon/circbuf v0.0.0-20150827004946-bbbad097214e // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/bgentry/speakeasy v0.1.0 // indirect
	github.com/fatih/color v1.9.0 // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/go-syslog v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/hashicorp/logutils v1.0.0 // indirect
	github.com/hashicorp/mdns v1.0.1 // indirect
	github.com/hashicorp/memberlist v0.2.2 // indirect
	github.com/hashicorp/serf v0.9.5 // indirect
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/mattn/go-pointer v0.0.1 // indirect
	github.com/miekg/dns v1.1.26 // indirect
	github.com/mitchellh/cli v1.1.0 // indirect
	github.com/mitchellh/mapstructure v0.0.0-20160808181253-ca63d7c062ee // indirect
	github.com/posener/complete v1.2.3 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	golang.org/x/crypto v0.0.0-20190923035154-9ee001bba392 // indirect
	golang.org/x/net v0.0.0-20190923162816-aa69164e4478 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000 // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000 // indirect
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000 // indirect
)
