DIR=/tmp/
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

install_all: compile pmdbserver proxyserver install docker_support

compile:
	echo "Compiling niovakv"

pmdbserver:
	cd pmdb/server && go build niovakv_pmdbserver.go

proxyserver:
	cd proxy && go build proxy.go

install:
	cp pmdb/server/niovakv_pmdbserver proxy/proxy ${DIR}/libexec/niova/
	
	cp proxy/config ${DIR}/libexec/niova/niovakv.config
	
	cp proxy/gossipNodes ${DIR}/libexec/niova/gossipNodes

docker_support:
	cp -r docker/* ${DIR}
	
	cp pmdb/server/niovakv_pmdbserver  ${DIR}/PMDBServerContents/

	cp proxy/proxy ${DIR}/ProxyContents/

	cp proxy/config ${DIR}/ProxyContents/niovakv.config
        
	cp proxy/gossipNodes ${DIR}/ProxyContents/gossipNodes

	cp -r ${DIR}/lib ${DIR}/raftconfig ${DIR}/ProxyContents

	cp -r ${DIR}/lib ${DIR}/raftconfig ${DIR}/PMDBServerContents
