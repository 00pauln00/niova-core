module niovakv/niovakv_pmdbserver

go 1.16

replace niova/go-pumicedb-lib/server => ../../../../server

replace common_libs/initlog => ../../common_go_libs

replace niova/go-pumicedb-lib/common => ../../../../common

replace niovakv/lib => ../../lib

require (
	common_libs/initlog v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
	niovakv/lib v0.0.0-00010101000000-000000000000
)
