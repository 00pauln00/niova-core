package httpserver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

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
	var niovaobj niovakvlib.NiovaKV
	switch r.Method {
	case "GET":
		reqBody, err := ioutil.ReadAll(r.Body)
		json.Unmarshal(reqBody, &niovaobj)
		if err != nil {
			log.Error("GET request failed: ", err)
		}
		log.Info("Data recieved using GET is :", niovaobj)
	case "PUT":
		r.ParseForm()
		reqBody, err := ioutil.ReadAll(r.Body)
		json.Unmarshal(reqBody, &niovaobj)
		if err != nil {
			log.Error("PUT request failed: ", err)
		}

		h.NKVCliObj.ReqObj = &niovaobj
		//Perform the read/write operation on pmdb client
		result, status := h.NKVCliObj.ProcessRequest()
		if status != 0 {
			log.Error("Operation failed: ", status)
		}
		log.Info("Result of the operation is:", result)

		resp := niovakvlib.NiovaKVResponse{
			RespStatus: status,
			RespValue:  result,
		}
		response, err := json.Marshal(&resp)
		_, errRes := fmt.Fprintf(w, "%s", response)
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
	h.server.Handler = h
	err := h.server.ListenAndServe()
	return err
}

//Close server
func (h HttpServerHandler) StopServer() error {
	err := h.server.Close()
	return err
}
