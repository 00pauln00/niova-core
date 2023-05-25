DIR=/usr/local/niova
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

COVID_PKGS=covidapp.com/covid_app_server,../../../server,../../../common,../lib
COVID_CLIENT_PKGS=covidapp.com/covid_app_client,../../../client,../../../common,../lib
FOODPALACE_PKGS=foodpalaceapp.com/foodpalaceappserver,../../../server,../../../common,../lib
FOODPALACE_CLIENT_PKGS=foodpalaceapp.com/foodpalaceappclient,../../../client,../../../common,../lib
NKV_PROXY_PKGS=controlplane/proxy,../../../../http/server,../../../../http/client,../requestResponseLib,../../../client,../../../common,../../../../serf/agent
NKVC_PKGS=niovakv/nkvc,../../../../serf/client,../../../common,../../../../http/client,../requestResponseLib,../../../../serf/serviceDiscovery,../../../../specificCompressionLib
NKV_PMDB_PKGS=common/pmdbServer,../../../server,../../../common,../../../../serf/agent,../requestResponseLib

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all: compile proxyInstall nkvcInstall pmdbserver covid_foodpalace_apps lkvt_module install

compile:
	echo "Compiling niovakv"

proxyInstall:
	cd proxy && go mod tidy && go build -cover -coverpkg=${NKV_PROXY_PKGS}

nkvcInstall:
	cd nkvc && go mod tidy && go build -coverpkg=${NKVC_PKGS}

pmdbserver:
	cd pmdbServer && go mod tidy && go build -cover -coverpkg=${NKV_PMDB_PKGS}

covid_foodpalace_apps:
	cd ../covid19APP/server && go mod tidy && go build -cover -coverpkg=${COVID_PKGS} && cd ../../niovaKV/ && cd ../covid19APP/client && go mod tidy && go build -cover -coverpkg=${COVID_CLIENT_PKGS} && cd ../../niovaKV/ && cd ../foodpalaceAPP/server && go mod tidy && go build -cover -coverpkg=${FOODPALACE_PKGS} && cd ../../niovaKV/ && cd ../foodpalaceAPP/client && go mod tidy && go build -cover -coverpkg=${FOODPALACE_CLIENT_PKGS}


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
