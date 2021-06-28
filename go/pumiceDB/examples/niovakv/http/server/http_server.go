package httpserver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"niovakv/niovakvlib"
	"niovakv/niovakvclient"
)

type HttpServerHandler struct {
	//Exported
	Addr       string
	Port       string
	NKVCliObj *niovakvclient.NiovaKVClient
	//Non-exported
	server http.Server
	rncui string
}

func (h HttpServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var niovaobj niovakvlib.NiovaKV
	switch r.Method {
	case "GET":
		reqBody, err := ioutil.ReadAll(r.Body)
		json.Unmarshal(reqBody, &niovaobj)
		if err != nil {
			fmt.Println("Data recieved using GET is :", niovaobj)
		}
	case "PUT":
		r.ParseForm()
		reqBody, err := ioutil.ReadAll(r.Body)
		json.Unmarshal(reqBody, &niovaobj)
		if err =! nil {
			fmt.Println("PUT request failed")
		}

		h.NKVCliObj.ReqObj = &niovaobj
		//Perform the read/write operation on pmdb client
		result, status := h.NKVCliObj.ProcessRequest()
		if status != 0 {
			fmt.Println("Operation failed.")
		}
		fmt.Fprintf(w, "result from httpserver is :", result, status)
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
