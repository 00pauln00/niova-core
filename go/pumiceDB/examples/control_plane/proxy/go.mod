module controlplane/proxy

replace niovakv/httpserver => ../../common/http/server

replace niovakv/niovakvlib => ../lib

replace niova/go-pumicedb-lib/client => ../../../client

replace niova/go-pumicedb-lib/common => ../../../common

replace niovakv/niovakvclient => ../pmdb/client

replace niovakvserver/serfagenthandler => ../../common/serf/agent

replace niovakv/niovakvpmdbclient => ../pmdb/client

go 1.16

require (
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niovakv/httpserver v0.0.0-00010101000000-000000000000
	niovakv/niovakvpmdbclient v0.0.0-00010101000000-000000000000
	niovakvserver/serfagenthandler v0.0.0-00010101000000-000000000000
)
