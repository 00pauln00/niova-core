module dictapp.com/pumice-dict-client

go 1.16

replace gopmdblib/goPmdbClient => ../../goPmdb/pumiceDBClient

replace gopmdblib/goPmdbCommon => ../../goPmdb/pumiceDBCommon

replace dictapplib/dict_libs => ../dict_libs

require (
	dictapplib/dict_libs v0.0.0-00010101000000-000000000000
	gopmdblib/goPmdbClient v0.0.0-00010101000000-000000000000
	gopmdblib/goPmdbCommon v0.0.0-00010101000000-000000000000
)
