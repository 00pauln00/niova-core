module niovactlplane/niovactlclient

go 1.16

replace niova/go-pumicedb-lib/client => ../../../client

replace niova/go-pumicedb-lib/common => ../../../common

replace niovactlplane/niovareqlib => ../lib

replace pmdbobj/reqqueue => ../queue

require (
	github.com/google/uuid v1.2.0 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.8.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
	niovactlplane/niovareqlib v0.0.0-00010101000000-000000000000
	pmdbobj/reqqueue v0.0.0-00010101000000-000000000000
)
