module niovakv/httpserver

go 1.16

replace niova/go-pumicedb-lib/client => ../../../../client

replace niova/go-pumicedb-lib/common => ../../../../common

replace niovakv/niovakvlib => ../../lib

replace niovakv/niovakvclient => ../../pmdb/client

require (
	niovakv/niovakvclient v0.0.0-00010101000000-000000000000
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000
)
