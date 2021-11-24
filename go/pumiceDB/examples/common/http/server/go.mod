module niovakv/httpserver

go 1.16

replace niova/go-pumicedb-lib/client => ../../../../client

replace niova/go-pumicedb-lib/common => ../../../../common

replace niovakv/niovakvlib => ../../../niovakv/lib

replace niovakv/niovakvpmdbclient => ../../../niovakv/pmdb/client

require (
	github.com/sirupsen/logrus v1.8.1
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000
	niovakv/niovakvpmdbclient v0.0.0-00010101000000-000000000000
)
