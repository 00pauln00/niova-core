module niovactlplane/niovactlclient

go 1.16

replace niova/go-pumicedb-lib/client => ../../../client

replace niova/go-pumicedb-lib/common => ../../../common

replace niovactlplane/niovareqlib => ../lib

require (
	github.com/google/uuid v1.2.0 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000 // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000 // indirect
	niovactlplane/niovareqlib v0.0.0-00010101000000-000000000000 // indirect
)
