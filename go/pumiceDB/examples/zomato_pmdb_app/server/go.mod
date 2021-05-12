module zomatoapp.com/zomato_app_server

go 1.16

replace niova/go-pumicedb-lib/server => ../../../server

replace niova/go-pumicedb-lib/common => ../../../common

replace zomatoapplib/lib => ../lib

require (
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000 // indirect
	zomatoapplib/lib v0.0.0-00010101000000-000000000000 // indirect
)
