DIR=/usr/local/niova
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all: compile proxyInstall nkvcInstall pmdbserver covid_foodpalace_apps lkvt_module install

compile:
	echo "Compiling niovakv"

proxyInstall:
	cd proxy && go mod tidy && go build

nkvcInstall:
	cd nkvc && go mod tidy && go build

pmdbserver:
	cd pmdbServer && go mod tidy && go build pmdbServer.go

covid_foodpalace_apps:
	cd ../covid19APP/server && go mod tidy && go build && cd ../../niovaKV/ && cd ../covid19APP/client && go mod tidy && go build && cd ../../niovaKV/ && cd ../foodpalaceAPP/server && go mod tidy && go build && cd ../../niovaKV/ && cd ../foodpalaceAPP/client && go mod tidy && go build


lkvt_module:
	cd lkvt && git submodule init && git submodule update && go build

install:
	cp pmdbServer/pmdbServer ${DIR}/libexec/niova/NKV_pmdbServer
	cp proxy/proxy ${DIR}/libexec/niova/NKV_proxy
	cp nkvc/nkvc lkvt/lkvt ${DIR}/libexec/niova/
	
	cp proxy/config ${DIR}/libexec/niova/niovakv.config
	cp ../covid19APP/server/covid_app_server ../covid19APP/client/covid_app_client ../covid19APP/client/vaccinations.csv ${DIR}/libexec/niova
	cp ../foodpalaceAPP/server/foodpalaceappserver ../foodpalaceAPP/client/foodpalaceappclient ../foodpalaceAPP/client/foodpalace.csv ${DIR}/libexec/niova
	cp docker/* ${DIR}
