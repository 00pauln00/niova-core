module covidapp.com/covid_app_client

go 1.16

replace gopmdblib/goPmdbClient => ../../goPmdb/pumiceDBClient

replace covidapp.com/covidapplib => ../covid19_libs

require (
	covidapp.com/covidapplib v0.0.0-00010101000000-000000000000
	github.com/satori/go.uuid v1.2.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopmdblib/goPmdbClient v0.0.0-00010101000000-000000000000
)
