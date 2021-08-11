module niovakv/niova_client

replace niovakv/serfclienthandler => ../../serf/client

replace niovakv/niovakvlib => ../../lib

replace niovakv/httpclient => ../../http/client

replace niovakv/clientapi => ../

replace niovakv/common => ../../../../common

go 1.16

require (
	github.com/sirupsen/logrus v1.8.1
	niovakv/clientapi v0.0.0-00010101000000-000000000000
	niovakv/common v0.0.0-00010101000000-000000000000 // indirect
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000
	niovakv/serfclienthandler v0.0.0-00010101000000-000000000000
)
