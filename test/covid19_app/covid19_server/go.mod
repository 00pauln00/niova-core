module covidapp.com/covid_app_server

go 1.16

replace gopmdblib/goPmdbServer => ../../goPmdb/pumiceDBServer

replace covidapp.com/covidapplib => ../covid19_libs

require (
	covidapp.com/covidapplib v0.0.0-00010101000000-000000000000
	gopmdblib/goPmdbServer v0.0.0-00010101000000-000000000000
)
