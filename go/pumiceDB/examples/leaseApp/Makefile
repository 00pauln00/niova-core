DIR=/tmp/
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

install_all: compile pmdbserver leaseclient install

compile:
	echo "Compiling leaseApplication"

pmdbserver:
	cd pmdbServer && go mod tidy  && go build pmdbServer.go

leaseclient:
	cd client &&  go mod tidy && go build leaseClient.go

install:
	cp pmdbServer/pmdbServer ${DIR}/libexec/niova/LeaseApp_pmdbServer
	
	cp client/leaseClient ${DIR}/libexec/niova/leaseClient

