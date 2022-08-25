DIR=/tmp

install_all: compile nisdMonitor install

compile:
	echo "Compiling lookout"

nisdMonitor:
	cd nisdMonitor && go mod tidy && go build

install:
	cp nisdMonitor/nisdLookout ${DIR}/libexec/niova/nisdLookout

clean:
	go clean
