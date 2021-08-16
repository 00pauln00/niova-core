CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/niova/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/
PATH=/usr/bin:/usr/local/go/bin

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all:
        compile pmdbserver

compile:
        echo "Compiling niovakv"
        
pmdbserver:
        cd pmdb/server && go build niovakv_pmdbserver.go
