module foodpalaceapp.com/foodpalaceappserver

go 1.16

replace niova/go-pumicedb-lib/server => ../../../server

replace niova/go-pumicedb-lib/common => ../../../common

replace foodpalaceapp.com/foodpalaceapplib => ../lib

require (
	foodpalaceapp.com/foodpalaceapplib v0.0.0-00010101000000-000000000000 // indirect
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000 // indirect
)
