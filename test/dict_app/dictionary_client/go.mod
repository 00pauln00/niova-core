module pumice-dict-client

go 1.15

replace gopmdblib/goPmdb => ../../goPmdb

replace dictapplib/dict_libs => ../dict_libs

require (
	dictapplib/dict_libs v0.0.0-00010101000000-000000000000
	github.com/mattn/go-pointer v0.0.1 // indirect
	gopmdblib/goPmdb v0.0.0-00010101000000-000000000000
)
