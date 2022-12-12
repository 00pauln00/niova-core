package main

import (
	"flag"
	"strconv"
	"strings"
	"time"
	"net/http"
	"net"

	log "github.com/sirupsen/logrus"
)

type testApplication struct {
	portRange             string
}

func (handler *testApplication) getCmdLineArgs() {
	flag.StringVar(&handler.portRange, "p", "NULL", "Port range [0-9]")
}

func (handler *testApplication) startHttpPort(){
	portRangeStart, err := strconv.Atoi(strings.Split(handler.portRange, "-")[0])
	portRangeEnd, err := strconv.Atoi(strings.Split(handler.portRange, "-")[1])
	if err != nil {
		log.Error(err)
	}
	for i := portRangeStart; i <= portRangeEnd; i++ {
		mux := http.NewServeMux()
		go func(i int) {
			port := strconv.Itoa(i)
			
			l, err := net.Listen("tcp", ":"+port)
	                if err != nil {
                                log.Error("Error while starting http on that port - ", err)
                	} else {
                        	go func() {
					http.Serve(l, mux)
                        	}()
               		}
		}(i)
		time.Sleep(1 * time.Second)	
	}
}

func main() {
	appHandler := testApplication{}
	appHandler.getCmdLineArgs()
	flag.Parse()
	appHandler.startHttpPort()
}
