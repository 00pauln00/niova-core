DIR=/usr/local/niova
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all: compile pmdbserver proxy nkvc covid_foodpalace_apps lkvt_module install

compile:
	echo "Compiling niovakv"

pmdbserver:
	cd pmdbServer && go build pmdbServer.go

proxy:
	cd proxy && go build proxy.go

nkvc:
	cd nkvc && go build nkvc.go

covid_foodpalace_apps:
	cd ../covid19APP/server && go build && cd ../../niovaKV/ && cd ../covid19APP/client && go build && cd ../../niovaKV/ && cd ../foodpalaceAPP/server && go build && cd ../../niovaKV/ && cd ../foodpalaceAPP/client && go build


lkvt_module:
	cd lkvt && git submodule init && git submodule update && git checkout control-plane && go build

install:
	sudo cp pmdbServer/pmdbServer proxy/proxy nkvc/nkvc lkvt/lkvt ${DIR}/libexec/niova
	sudo cp proxy/config ${DIR}/libexec/niova/niovakv.config
	sudo cp ../covid19APP/server/covid_app_server ../covid19APP/client/covid_app_client ../covid19APP/client/vaccinations.csv ${DIR}/libexec/niova
	sudo cp ../foodpalaceAPP/server/foodpalaceappserver ../foodpalaceAPP/client/foodpalaceappclient ../foodpalaceAPP/client/foodpalace.csv ${DIR}/libexec/niova
	sudo cp docker/* ${DIR}
