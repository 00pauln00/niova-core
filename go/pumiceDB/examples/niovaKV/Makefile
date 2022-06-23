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
	cd proxy && go build

nkvcInstall:
	cd nkvc && go build

pmdbserver:
	cd pmdbServer && go build pmdbServer.go

covid_foodpalace_apps:
	cd ../covid19APP/server && go build && cd ../../niovaKV/ && cd ../covid19APP/client && go build && cd ../../niovaKV/ && cd ../foodpalaceAPP/server && go build && cd ../../niovaKV/ && cd ../foodpalaceAPP/client && go build


lkvt_module:
	cd lkvt && git submodule init && git submodule update && git checkout golang_build_fix && go build

install:
	sudo cp pmdbServer/pmdbServer ${DIR}/libexec/niova/NKV_pmdbServer
	sudo cp proxy/proxy ${DIR}/libexec/niova/NKV_proxy
	sudo cp nkvc/nkvc lkvt/lkvt ${DIR}/libexec/niova/
	
	sudo cp proxy/config ${DIR}/libexec/niova/niovakv.config
	sudo cp ../covid19APP/server/covid_app_server ../covid19APP/client/covid_app_client ../covid19APP/client/vaccinations.csv ${DIR}/libexec/niova
	sudo cp ../foodpalaceAPP/server/foodpalaceappserver ../foodpalaceAPP/client/foodpalaceappclient ../foodpalaceAPP/client/foodpalace.csv ${DIR}/libexec/niova
	sudo cp docker/* ${DIR}
