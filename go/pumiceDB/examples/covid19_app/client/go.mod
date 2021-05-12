module covidapp.com/covid_app_client

go 1.16

replace niova/go-pumicedb-lib/client => ../../../client

replace niova/go-pumicedb-lib/common => ../../../common

replace covidapplib/lib => ../lib

require (
	covidapplib/lib v0.0.0-00010101000000-000000000000
	github.com/satori/go.uuid v1.2.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)
