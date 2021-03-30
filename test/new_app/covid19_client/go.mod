module covidapp.com/pumice-app-client

go 1.16

replace pmdblib/goPmdb => ../../goPmdb

replace covidapp.com/covidapplib => ../covid19_libs

require (
	covidapp.com/covidapplib v0.0.0-00010101000000-000000000000
	github.com/mattn/go-pointer v0.0.1 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	pmdblib/goPmdb v0.0.0-00010101000000-000000000000
)
