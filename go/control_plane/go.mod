module niovakv/make

go 1.16

replace niova/go-pumicedb-lib/server => ../../server

replace niovakv/niovakvlib => ./lib

replace niovakv/niovakvpmdbclient => ./pmdb/client

replace niova/go-pumicedb-lib/client => ../../client

replace niova/go-pumicedb-lib/common => ../../common

replace niovakv/httpserver => ../common/http/server

replace niovakvserver/serfagenthandler => ./serf/agent

replace niovakv/serfclienthandler => ./serf/client

replace niovakv/httpclient => ./http/client

require (
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000 // indirect
	niovakv/httpclient v0.0.0-00010101000000-000000000000 // indirect
	niovakv/httpserver v0.0.0-00010101000000-000000000000 // indirect
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000 // indirect
	niovakv/niovakvpmdbclient v0.0.0-00010101000000-000000000000 // indirect
	niovakv/serfclienthandler v0.0.0-00010101000000-000000000000 // indirect
	niovakvserver/serfagenthandler v0.0.0-00010101000000-000000000000 // indirect
)
