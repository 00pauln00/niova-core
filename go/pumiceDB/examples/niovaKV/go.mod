module niovakv/make

go 1.16

replace niova/go-pumicedb-lib/server => ../../server

replace niovakv/niovakvlib => ./requestResponseLib

replace niovakv/niovakvpmdbclient => ../../client

replace niova/go-pumicedb-lib/client => ../../client

replace niova/go-pumicedb-lib/common => ../../common

replace niovakv/httpserver => ../../../http/server

replace niovakvserver/serfagenthandler => ../../../serf/agent

replace niovakv/serfclienthandler => ../../../serf/client

replace niovakv/httpclient => ../../../http/client

require (
	github.com/aybabtme/uniplot v0.0.0-20151203143629-039c559e5e7e // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
)
