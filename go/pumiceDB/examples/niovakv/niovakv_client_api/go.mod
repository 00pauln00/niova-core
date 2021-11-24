module niovakv/clientapi

replace niovakv/serfclienthandler => ../../common/serf/client

replace niova/go-pumicedb-lib/common => ../../../common

replace niovakv/httpclient => ../../common/http/client

replace niovakv/niovakvlib => ../lib

go 1.16

require (
	github.com/hashicorp/serf v0.9.5
	github.com/sirupsen/logrus v1.8.1
	niovakv/httpclient v0.0.0-00010101000000-000000000000
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000
	niovakv/serfclienthandler v0.0.0-00010101000000-000000000000
)
