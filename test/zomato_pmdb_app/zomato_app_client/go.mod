module zomatoapp/zomato_app_client

go 1.16

replace gopmdblib/goPmdbClient => ../../goPmdb/pumiceDBClient

replace zomatoapp/zomatoapplib => ../zomatoapplib

require (
	github.com/mattn/go-pointer v0.0.1 // indirect
	github.com/satori/go.uuid v1.2.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopmdblib/goPmdbClient v0.0.0-00010101000000-000000000000
	zomatoapp/zomatoapplib v0.0.0-00010101000000-000000000000 // indirect
)
