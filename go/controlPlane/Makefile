DIR=/tmp/
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

install_all: compile pmdbserver proxyserver ncpcclient configapp install docker_support

install_only: compile pmdbserver proxyserver ncpcclient configapp testapp install

compile:
	echo "Compiling controlPlane"

pmdbserver:
	cd pmdbServer && go mod tidy  && go build pmdbServer.go

proxyserver:
	cd proxy && go mod tidy  && go build proxy.go

ncpcclient:
	cd ncpc &&  go mod tidy && go build ncpc.go

configapp:
	cd configApplication && go mod tidy && go build configApplication.go

testapp:
	cd testApplication && go mod tidy && go build testApplication.go

install:
	cp pmdbServer/pmdbServer ${DIR}/libexec/niova/CTLPlane_pmdbServer

	cp proxy/proxy ${DIR}/libexec/niova/CTLPlane_proxy
	
	cp ncpc/ncpc ${DIR}/libexec/niova/ncpc

	cp proxy/config ${DIR}/libexec/niova/niovakv.config

	cp proxy/gossipNodes ${DIR}/libexec/niova/gossipNodes

	cp configApplication/configApplication ${DIR}/libexec/niova/cfgApp

	cp testApplication/testApplication ${DIR}/libexec/niova/testApp

docker_support:
	cp -r docker/* ${DIR}
	
	cp pmdbServer/pmdbServer  ${DIR}/PMDBServerContents/CTLPlane_pmdbServer

	cp proxy/gossipNodes ${DIR}/PMDBServerContents/

	cp proxy/proxy ${DIR}/ProxyContents/CTLPlane_proxy

	cp ncpc/ncpc ${DIR}/ProxyContents/

	cp proxy/config ${DIR}/ProxyContents/niovakv.config

	cp proxy/gossipNodes ${DIR}/ProxyContents/gossipNodes

	cp -r ${DIR}/lib ${DIR}/raftconfig ${DIR}/ProxyContents

	cp -r ${DIR}/lib ${DIR}/raftconfig ${DIR}/PMDBServerContents
