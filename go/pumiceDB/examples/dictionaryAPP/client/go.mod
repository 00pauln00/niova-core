module dictapp.com/pumice-dict-client 

go 1.16

replace niova/go-pumicedb-lib/client => ../../../client
replace niova/go-pumicedb-lib/common => ../../../common

replace dictapplib/lib => ../lib

require (
	dictapplib/lib v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.2.0 // indirect
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)
