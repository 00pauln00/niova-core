module niovakv/niovakvexec

go 1.16

replace httpserver.com/httpserver => ../http/server

replace niovakv/niovakvlib => ../lib

replace niova/go-pumicedb-lib/client => ../../../client

replace niova/go-pumicedb-lib/common => ../../../common

replace niovakv/niovakvclient => ../pmdb/client

replace niovakvserver/serfagenthandler => ../serf/agent

require (
	httpserver.com/httpserver v0.0.0-00010101000000-000000000000
	niovakv/niovakvclient v0.0.0-00010101000000-000000000000
	niovakvserver/serfagenthandler v0.0.0-00010101000000-000000000000
)
