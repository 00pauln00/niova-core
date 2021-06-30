package httpserver

import (
	"bytes"
	"encoding/gob"
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
	fmt.Println("New request")
	var requestobj niovakvlib.NiovaKV
	switch r.Method {
	case "GET":
		reqBody, err := ioutil.ReadAll(r.Body)
		dec := gob.NewDecoder(bytes.NewBuffer(reqBody))
		err = dec.Decode(&requestobj)
		if err != nil {
			log.Error("GET request failed: ", err)
		}
		//Perform the read/write operation on pmdb client
		result, err := h.NKVCliObj.ProcessRequest(&requestobj)
		if err != nil {
			log.Error("Operation failed: ", err)
		} else {
			log.Info("Result of the operation is:", result)
			resp := niovakvlib.NiovaKVResponse{
				RespStatus: 0,
				RespValue:  result,
			}
			var response bytes.Buffer
			enc := gob.NewEncoder(&response)
			err = enc.Encode(resp)
			_, errRes := fmt.Fprintf(w, "%s", response.String())
			if errRes != nil {
				log.Error(errRes)
			}
		}

	case "PUT":
		r.ParseForm()
		reqBody, err := ioutil.ReadAll(r.Body)

		dec := gob.NewDecoder(bytes.NewBuffer(reqBody))
		err = dec.Decode(&requestobj)
		if err != nil {
			log.Error("PUT request failed: ", err)
		}

		//Perform the read/write operation on pmdb client
		result, err := h.NKVCliObj.ProcessRequest(&requestobj)
		if err != nil {
			log.Error("Operation failed: ", err)
		} else {
			log.Info("Result of the operation is:", result)

			resp := niovakvlib.NiovaKVResponse{
				RespStatus: 0,
				RespValue:  result,
			}
			var response bytes.Buffer
			enc := gob.NewEncoder(&response)
			err = enc.Encode(resp)
			_, errRes := fmt.Fprintf(w, "%s", response.String())
			if errRes != nil {
				log.Error(errRes)
			}
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
