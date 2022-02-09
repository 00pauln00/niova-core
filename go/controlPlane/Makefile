DIR=/tmp/
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

install_all: compile pmdbserver proxyserver ncpcclient install docker_support

install_only: compile pmdbserver proxyserver ncpcclient install

compile:
	echo "Compiling niovakv"

pmdbserver:
	cd PMDBServer && go build pmdbServer.go

proxyserver:
	cd proxy && go build proxy.go

ncpcclient:
	cd ncpc && go build ncpc.go

install:
	cp ../common/PMDB/server/pmdbServer proxy/proxy ncpc/ncpc ${DIR}/libexec/niova/
	
	cp proxy/config ${DIR}/libexec/niova/niovakv.config

	cp proxy/gossipNodes ${DIR}/libexec/niova/gossipNodes

docker_support:
	cp -r docker/* ${DIR}
	
	cp ../common/PMDB/server/pmdbServer  ${DIR}/PMDBServerContents/

	cp proxy/gossipNodes ${DIR}/PMDBServerContents/

	cp proxy/proxy ${DIR}/ProxyContents/

	cp ncpc/ncpc ${DIR}/ProxyContents/

	cp proxy/config ${DIR}/ProxyContents/niovakv.config

	cp proxy/gossipNodes ${DIR}/ProxyContents/gossipNodes

	cp -r ${DIR}/lib ${DIR}/raftconfig ${DIR}/ProxyContents

	cp -r ${DIR}/lib ${DIR}/raftconfig ${DIR}/PMDBServerContents
