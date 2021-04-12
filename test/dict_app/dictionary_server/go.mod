module dictapp.com/pumice-dict-server

go 1.16

replace gopmdblib/goPmdbServer => ../../goPmdb/pumiceDBServer

replace dictapplib/dict_libs => ../dict_libs

require (
	dictapplib/dict_libs v0.0.0-00010101000000-000000000000
	gopmdblib/goPmdbServer v0.0.0-00010101000000-000000000000
)
