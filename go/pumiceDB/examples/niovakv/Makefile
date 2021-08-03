CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/
PATH=/usr/bin:/usr/local/go/bin

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all: compile pmdbserver niovakvserver niovakvclient install

compile:
	echo "Compiling niovakv"

pmdbserver:
	cd pmdb/server && go build niovakv_pmdbserver.go

niovakvserver:
	cd niovakv_server && go build niovakv_server.go

niovakvclient:
	cd niovakv_client && go build niovakv_client.go
	cd niovakv_client_concurrent && go build niovakv_client_concurrent.go

install:
	cp pmdb/server/niovakv_pmdbserver niovakv_server/niovakv_server niovakv_client/niovakv_client niovakv_client_concurrent/niovakv_client_concurrent ${DIR}/libexec/niova
	cp niovakv_server/config ${DIR}/libexec/niova/niovakv.config
