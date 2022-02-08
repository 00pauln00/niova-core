module common/httpServer

go 1.16

replace niova/go-pumicedb-lib/client => ../../pumiceDB/client

replace niova/go-pumicedb-lib/common => ../../pumiceDB/common

replace common/requestResponseLib => ../../RequestResponselib

//replace common/pmdbClient => ../../PMDB/client

//replace niova/go-pumicedb-lib/client => ../../../../client

require (
	//common/pmdbClient v0.0.0-00010101000000-000000000000 // indirect
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000 // indirect
)
