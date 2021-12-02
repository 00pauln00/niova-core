module niovakv/niovakv_pmdbserver

go 1.16

replace niova/go-pumicedb-lib/server => ../../../../server

replace niova/go-pumicedb-lib/common => ../../../../common

replace pmdbServer/serfagenthandler => ../../../common/serf/agent

replace niovakv/niovakvlib => ../../lib

require (
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000
	pmdbServer/serfagenthandler v0.0.0-00010101000000-000000000000 // indirect
)
