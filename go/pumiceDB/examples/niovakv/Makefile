CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/
PATH=/usr/bin:/usr/local/go/bin

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all: compile pmdbserver niovakvserver niovakvclient copy_config

compile:
	echo "Compiling niovakv"

pmdbserver:
	go build -o ${DIR}/pmdbserver pmdb/server/niovakv_pmdbserver.go

niovakvserver:
	go build -o ${DIR}/niovakv_server niovakv_server/niovakv_server.go

niovakvclient:
	go build -o ${DIR}/niovakv_client niovakv_client/niovakv_client.go
	go build -o ${DIR}/niovakv_client_concurrent niovakv_client_concurrent/niovakv_client_concurrent.go

copy_config:
	cp niovakv_server/config ${DIR}
