DIR=/usr/local/niova
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all: compile pmdbserver proxy nkvc lkvt_module covid_foodpalace_apps install

compile:
	echo "Compiling niovakv"

pmdbserver:
	cd PMDBServer && go build pmdbServer.go

proxy:
	cd proxy && go build proxy.go

nkvc:
	cd nkvc && go build nkvc.go

covid_foodpalace_apps:
	cd ../covid19_app/server && go build && cd ../../niovakv/ && cd ../covid19_app/client && go build && cd ../../niovakv/ && cd ../foodpalace_app/server && go build && cd ../../niovakv/ && cd ../foodpalace_app/client && go build


lkvt_module:
	cd lkvt && git submodule init && git submodule update && git checkout control-plane && go build

install:
	sudo cp pmdb/server/niovakv_pmdbserver niovakv_server/niovakv_server niovakv_client/niovakv_client lkvt/lkvt ${DIR}/libexec/niova
	sudo cp niovakv_server/config ${DIR}/libexec/niova/niovakv.config
	sudo cp ../covid19_app/server/covid_app_server ../covid19_app/client/covid_app_client ../covid19_app/client/vaccinations.csv ${DIR}/libexec/niova
	sudo cp ../foodpalace_app/server/foodpalaceappserver ../foodpalace_app/client/foodpalaceappclient ../foodpalace_app/client/foodpalace.csv ${DIR}/libexec/niova
	sudo cp docker/* ${DIR}
