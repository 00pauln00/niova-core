package httpServer

import (
	//"bytes"
	//"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	//"common/requestResponseLib"
	"common/pmdbClient"
	"time"
	"sync"
	//"encoding/hex"
	"sync/atomic"
	log "github.com/sirupsen/logrus"
)

type HTTPServerHandler struct {
	//Exported
	Addr			string
	Port			string
	PMDBClientHandlerObj    *pmdbClient.PMDBClientHandler
	HTTPConnectionLimit     int
	PMDBServerConfig	map[string][]byte

	//Non-exported
	HTTPServer            http.Server
	rncui	              string
	connectionLimiter     chan int

	//For stats
	StatsRequired bool
	Stat          HTTPServerStat
	statLock      sync.Mutex
}

type HTTPServerStat struct {
        GetCount        int64
        PutCount        int64
	GetSuccessCount int64
        PutSuccessCount int64
	Queued          int64
	ReceivedCount   int64
	FinishedCount   int64
	syncRequest     int64
	StatusMap       map[int64]*RequestStatus
}

type RequestStatus struct {
	RequestHash string
	Status	    string
}

/*
func (h *HttpServerHandler) process(r *http.Request, requestStat *RequestStatus) ([]byte, error,bool) {
	var requestobj niovakvlib.NiovaKV
	var resp niovakvlib.NiovaKVResponse
	var response bytes.Buffer
	var success bool
	var err error

	reqBody, err := ioutil.ReadAll(r.Body)
	if err == nil {
		dec := gob.NewDecoder(bytes.NewBuffer(reqBody))
		err = dec.Decode(&requestobj)
	}

	if h.NeedStats {
		requestStat.RequestHash = hex.EncodeToString(requestobj.CheckSum[:])
		requestStat.Status = "processing"
		atomic.AddInt64(&h.Stat.Queued,int64(-1))
	}

	//If error in parsing request
	if err != nil {
		log.Error("(HTTP Server) Parsing request failed: ", err)
		resp.RespStatus = -1
		resp.RespValue = []byte("Parsing request failed")
	} else {
		//Perform the read operation on pmdb client
		log.Trace("(HTTP Server) Received request; operation : ", requestobj.InputOps, " Key : ", requestobj.InputKey, " Value : ", requestobj.InputValue)
		result, err := h.NKVCliObj.ProcessRequest(&requestobj)
		//If operation failed
		if err != nil {
			resp.RespStatus = -1
			resp.RespValue = []byte(err.Error())
		} else {
			resp.RespStatus = 0
			resp.RespValue = result
			success = true
		}
	}
	enc := gob.NewEncoder(&response)
	err = enc.Encode(resp)
	return response.Bytes(), err, success
}
*/

func (handler *HTTPServerHandler) ServeHTTP(writer http.ResponseWriter, reader *http.Request) {
	//Go followes causually consistent memory model, so require sync among stat and normal request to get consistent stat data
	atomic.AddInt64(&handler.Stat.syncRequest,int64(1))

	if (reader.URL.Path == "/config") {
		uuid, err := ioutil.ReadAll(reader.Body)
		if err!=nil{
			fmt.Fprintf(writer,"Unable to get the uuid")
		}
		data := handler.PMDBServerConfig[string(uuid)]
		fmt.Fprintf(writer,"%s",data)
		return
	}

	if (reader.URL.Path == "/stat") && (handler.StatsRequired) {
		log.Trace(handler.Stat)
		handler.statLock.Lock()
		stat, err := json.MarshalIndent(handler.Stat, "", " ")
		handler.statLock.Unlock()
		if err != nil {
                        log.Error("(HTTP Server) Writing to http response writer failed :", err)
                }
		_, err = fmt.Fprintf(writer, "%s", string(stat))
                if err != nil {
			log.Error("(HTTP Server) Writing to http response writer failed :", err)
                }
		return
	}


	var thisRequestStat *RequestStatus
	var id int64
	if (handler.StatsRequired) {
		handler.statLock.Lock()
		handler.Stat.ReceivedCount += 1
		id = handler.Stat.ReceivedCount
		handler.Stat.Queued += 1
		thisRequestStat = &RequestStatus{
			Status : "Queued",
		}
		handler.Stat.StatusMap[id] = thisRequestStat
		handler.statLock.Unlock()
	}

	//Limits no of HTTP connections
	handler.connectionLimiter <- 1
	defer func() {
		<-handler.connectionLimiter
	}()

	var success bool
	var result []byte
	var err error
	requestBytes, err := ioutil.ReadAll(reader.Body)
	switch reader.Method {
	case "GET":
		thisRequestStat.Status = "Processing"
		result, err = handler.PMDBClientHandlerObj.Read(requestBytes)
		fallthrough
	case "PUT":
		if thisRequestStat.Status == "Queued" {
			thisRequestStat.Status = "Processing"
			result, err = handler.PMDBClientHandlerObj.Write(requestBytes)
		}
		if err == nil {
			success = true
		}
		fmt.Fprintf(writer, "%s", string(result))
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}

	//Update status
	if handler.StatsRequired{
		handler.statLock.Lock()
		delete(handler.Stat.StatusMap,id)
		handler.Stat.FinishedCount += int64(1)
		switch reader.Method{
		case "GET":
			handler.Stat.GetCount += int64(1)
			if success{
				handler.Stat.GetSuccessCount += int64(1)
			}
		case "PUT":
                        handler.Stat.PutCount += int64(1)
                        if success{
                                handler.Stat.PutSuccessCount += int64(1)
                        }
		}
		handler.statLock.Unlock()
	}

}


func (handler *HTTPServerHandler) Start_HTTPServer() error {
	handler.connectionLimiter = make(chan int, handler.HTTPConnectionLimit)
	handler.HTTPServer = http.Server{}
	handler.HTTPServer.Addr = handler.Addr + ":" + handler.Port

	//Update the timeout using little's fourmula
	handler.HTTPServer.Handler = http.TimeoutHandler(handler, 150*time.Second, "Server Timeout")
	handler.Stat.StatusMap = make(map[int64]*RequestStatus)
	err := handler.HTTPServer.ListenAndServe()
	return err
}

//Close server
func (h HTTPServerHandler) Stop_HTTPServer() error {
	err := h.HTTPServer.Close()
	return err
}
