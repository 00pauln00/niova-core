module zomatoapp.com/zomato_app_server

go 1.16

replace gopmdblib/goPmdbServer => ../../goPmdb/pumiceDBServer

replace gopmdblib/goPmdbCommon => ../../goPmdb/pumiceDBCommon

replace zomatoapp.com/zomatolib => ../zomatoapplib

require (
	gopmdblib/goPmdbCommon v0.0.0-00010101000000-000000000000
	gopmdblib/goPmdbServer v0.0.0-00010101000000-000000000000
	zomatoapp.com/zomatolib v0.0.0-00010101000000-000000000000
)
