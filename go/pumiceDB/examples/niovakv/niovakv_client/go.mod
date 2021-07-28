module niovakv/niovakv_client

replace niovakv/serfclienthandler => ../serf/client

replace common_libs/initlog => ../common_go_libs

replace niovakv/httpclient => ../http/client

replace niovakv/niovakvlib => ../lib

go 1.16

require (
	common_libs/initlog v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.8.1
	niovakv/httpclient v0.0.0-00010101000000-000000000000
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000
	niovakv/serfclienthandler v0.0.0-00010101000000-000000000000
)
