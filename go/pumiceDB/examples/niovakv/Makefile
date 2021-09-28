DIR=/usr/local/niova
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all: compile pmdbserver niovakvserver niovakvclient lkvt_module install

compile:
	echo "Compiling niovakv"

pmdbserver:
	cd pmdb/server && go build niovakv_pmdbserver.go

niovakvserver:
	cd niovakv_server && go build niovakv_server.go

niovakvclient:
	cd niovakv_client && go build niovakv_client.go

lkvt_module:
	cd lkvt && git submodule init && git submodule update && go build

install:
	sudo cp pmdb/server/niovakv_pmdbserver niovakv_server/niovakv_server niovakv_client/niovakv_client lkvt/lkvt ${DIR}/libexec/niova
	sudo cp niovakv_server/config ${DIR}/libexec/niova/niovakv.config
