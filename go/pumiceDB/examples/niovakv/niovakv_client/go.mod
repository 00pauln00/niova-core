module niovakv/niovakv_client

replace niovakv/serfclienthandler => ../../common/serf/client

replace niova/go-pumicedb-lib/common => ../../../common

replace niovakv/httpclient => ../../common/http/client

replace niovakv/niovakvlib => ../lib

replace niovakv/clientapi => ../niovakv_client_api

go 1.16

require (
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niovakv/clientapi v0.0.0-00010101000000-000000000000
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000
)
