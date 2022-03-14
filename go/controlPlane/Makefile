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
	cd services/pmdbServer && go build

proxyserver:
	cd services/proxy && go build

ncpcclient:
	cd services/ncpc && go build

install:
	cp services/pmdbServer/pmdbServer services/proxy/proxy services/ncpc/ncpc ${DIR}/libexec/niova/
	
	cp services/proxy/config ${DIR}/libexec/niova/niovakv.config

	cp services/proxy/gossipNodes ${DIR}/libexec/niova/gossipNodes

docker_support:
	cp -r docker/* ${DIR}
	
	cp services/pmdbServer/pmdbServer  ${DIR}/PMDBServerContents/

	cp services/proxy/gossipNodes ${DIR}/PMDBServerContents/

	cp services/proxy/proxy ${DIR}/ProxyContents/

	cp services/ncpc/ncpc ${DIR}/ProxyContents/

	cp services/proxy/config ${DIR}/ProxyContents/niovakv.config

	cp services/proxy/gossipNodes ${DIR}/ProxyContents/gossipNodes

	cp -r ${DIR}/lib ${DIR}/raftconfig ${DIR}/ProxyContents

	cp -r ${DIR}/lib ${DIR}/raftconfig ${DIR}/PMDBServerContents
