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
	//Non-exported
	server http.Server
	rncui  string
}

func (h HttpServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var requestobj niovakvlib.NiovaKV
	var resp niovakvlib.NiovaKVResponse
	var response bytes.Buffer

	switch r.Method {
	case "GET":
		reqBody, err := ioutil.ReadAll(r.Body)
		dec := gob.NewDecoder(bytes.NewBuffer(reqBody))
		err = dec.Decode(&requestobj)

		//If error in parsing request
		if err != nil {
			log.Error("GET request failed: ", err)
			resp.RespStatus = -1
			resp.RespValue = []byte("Parsing request failed")
		} else {
			//Perform the read operation on pmdb client
			log.Info("Received read request for key ", requestobj.InputKey)
			result, err := h.NKVCliObj.ProcessRequest(&requestobj)
			//If operation failed
			if err != nil {
				log.Error("Read operation failed for key with error: ", requestobj.InputKey, err)
				resp.RespStatus = -1
				resp.RespValue = []byte(err.Error())
			} else {
				log.Info("Result of the read operation for key is:", requestobj.InputKey, result)
				resp.RespStatus = 0
				resp.RespValue = result
			}
		}
		enc := gob.NewEncoder(&response)
		err = enc.Encode(resp)
		_, errRes := fmt.Fprintf(w, "%s", response.String())
		if errRes != nil {
			log.Error(errRes)
		}

	case "PUT":
		//r.ParseForm()
		reqBody, err := ioutil.ReadAll(r.Body)
		dec := gob.NewDecoder(bytes.NewBuffer(reqBody))
		err = dec.Decode(&requestobj)

		//If error in parsing request
		if err != nil {
			log.Error("POST request failed: ", err)
			resp.RespStatus = -1
			resp.RespValue = []byte("Parsing request failed")
		} else {
			//Perform the read operation on pmdb client
			log.Info("Received write request for key with value", requestobj.InputKey, requestobj.InputValue)
			result, err := h.NKVCliObj.ProcessRequest(&requestobj)
			//If operation failed
			if err != nil {
				log.Error("Write operation failed for key with error: ", requestobj.InputKey, err)
				resp.RespStatus = -1
				resp.RespValue = []byte(err.Error())
			} else {
				log.Info("Status of the write operation for key is successful:", requestobj.InputKey)
				resp.RespStatus = 0
				resp.RespValue = result
			}
		}
		enc := gob.NewEncoder(&response)
		err = enc.Encode(resp)
		_, errRes := fmt.Fprintf(w, "%s", response.String())
		if errRes != nil {
			log.Error(errRes)
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

//Blocking func
func (h HttpServerHandler) StartServer() error {

	h.server = http.Server{}
	h.server.Addr = h.Addr + ":" + h.Port
	h.server.Handler = http.TimeoutHandler(h, 30*time.Second, "Server Timeout")
	err := h.server.ListenAndServe()
	return err
}

//Close server
func (h HttpServerHandler) StopServer() error {
	err := h.server.Close()
	return err
}
