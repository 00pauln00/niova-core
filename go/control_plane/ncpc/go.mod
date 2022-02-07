module ctlplane/ncpc

replace common/serfClient => ../../Serf/client

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace common/httpClient => ../../HTTP/client

replace common/requestResponseLib => ../RequestResponselib

replace common/clientAPI => ../../Serf/ClientAPI

go 1.17

require (
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)

require (
	common/clientAPI v0.0.0-00010101000000-000000000000
	common/requestResponseLib v0.0.0-00010101000000-000000000000
)
