package httpserver

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"

	"niovakv/niovakvlib"
	"niovakv/niovakvpmdbclient"
)

type HttpServerHandler struct {
	//Exported
	Addr      string
	Port      string
	NKVCliObj *niovakvpmdbclient.NiovaKVClient
	Limit     int
	//Non-exported
	server  http.Server
	rncui   string
	limiter chan int
}

func (h HttpServerHandler) process(r *http.Request) ([]byte, error) {
	var requestobj niovakvlib.NiovaKV
	var resp niovakvlib.NiovaKVResponse
	var response bytes.Buffer

	reqBody, err := ioutil.ReadAll(r.Body)
	dec := gob.NewDecoder(bytes.NewBuffer(reqBody))
	err = dec.Decode(&requestobj)

	//If error in parsing request
	if err != nil {
		log.Error("Request failed: ", err)
		resp.RespStatus = -1
		resp.RespValue = []byte("Parsing request failed")
	} else {
		//Perform the read operation on pmdb client
		log.Info("Received request, operation : ", requestobj.InputOps, " Key : ", requestobj.InputKey, " Value : ", requestobj.InputValue)
		result, err := h.NKVCliObj.ProcessRequest(&requestobj)
		//If operation failed
		if err != nil {
			log.Error("Operation failed for key with error: ", requestobj.InputKey, " ", err)
			resp.RespStatus = -1
			resp.RespValue = []byte(err.Error())
		} else {
			log.Info("Result of the operation is:", requestobj.InputKey, " ", result)
			resp.RespStatus = 0
			resp.RespValue = result
		}
	}
	enc := gob.NewEncoder(&response)
	err = enc.Encode(resp)
	return response.Bytes()
}

func (h HttpServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//Blocks if more no specified no of request is already in the queue
	h.limiter <- 1
	defer func() {
		<-h.limiter
	}()
	switch r.Method {
	case "GET":
		fallthrough
	case "PUT":
		respString := h.process(r)
		_, errRes := fmt.Fprintf(w, "%s", string(respString))
		if errRes != nil {
			log.Error("Error:", errRes)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

//Blocking func
func (h HttpServerHandler) StartServer() error {
	h.limiter = make(chan int, h.Limit)
	h.server = http.Server{}
	h.server.Addr = h.Addr + ":" + h.Port
	h.server.Handler = http.TimeoutHandler(h, 150*time.Second, "Server Timeout")
	err := h.server.ListenAndServe()
	return err
}

//Close server
func (h HttpServerHandler) StopServer() error {
	err := h.server.Close()
	return err
}
