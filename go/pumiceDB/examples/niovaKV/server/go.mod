module niovactlplane/niovaKVServer

go 1.16

replace niova/go-pumicedb-lib/server => ../../../server

replace niova/go-pumicedb-lib/common => ../../../common

replace niovactlplane/lib => ../lib

require (
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
	niovactlplane/lib v0.0.0-00010101000000-000000000000
)
