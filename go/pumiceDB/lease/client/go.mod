module LeaseLib/LeaseClient

replace common/leaseLib => ../common

replace niova/go-pumicedb-lib/client => ../../client

replace niova/go-pumicedb-lib/common => ../../common

replace common/httpClient => ../../../http/client

replace common/clientAPI => ../../../serf/serviceDiscovery

replace common/specificCompressionLib => ../../../specificCompressionLib

replace common/serfClient => ../../../serf/client

go 1.18

require (
	common/clientAPI v0.0.0-00010101000000-000000000000
	common/leaseLib v0.0.0-00010101000000-000000000000
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.9.0
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)

require (
	common/httpClient v0.0.0-00010101000000-000000000000 // indirect
	common/serfClient v0.0.0-00010101000000-000000000000 // indirect
	common/specificCompressionLib v0.0.0-00010101000000-000000000000 // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/hashicorp/logutils v1.0.0 // indirect
	github.com/hashicorp/memberlist v0.3.0 // indirect
	github.com/hashicorp/serf v0.9.7 // indirect
	github.com/miekg/dns v1.1.41 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	golang.org/x/net v0.0.0-20210410081132-afb366fc7cd1 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
)
