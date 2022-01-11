module common/httpServer

go 1.16

replace niova/go-pumicedb-lib/client => ../../../../client

replace niova/go-pumicedb-lib/common => ../../../../common

replace common/requestResponseLib => ../../RequestResponselib

replace common/pmdbClient => ../../PMDB/client

require (
	common/pmdbClient v0.0.0-00010101000000-000000000000 // indirect
	github.com/sirupsen/logrus v1.8.1
)
