module common/clientAPI

replace common/serfClient => ../client

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace common/httpClient => ../../HTTP/client

go 1.17

require (
	common/httpClient v0.0.0-00010101000000-000000000000
	common/serfClient v0.0.0-00010101000000-000000000000
	github.com/hashicorp/serf v0.9.6
	github.com/sirupsen/logrus v1.8.1
)
