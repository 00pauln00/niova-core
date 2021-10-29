package httpserver

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"niovakv/niovakvlib"
	"niovakv/niovakvpmdbclient"
	"time"
	"sync"
	"sync/atomic"
	log "github.com/sirupsen/logrus"
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

	//For stats
	NeedStats bool
	Stat      HttpServerStat
	statLock  sync.Mutex
}

type HttpServerStat struct {
        GetCount int64
        PutCount int64
	GetSuccessCount int64
        PutSuccessCount int64
	Queued int64
	ReceivedCount int64
	FinishedCount int64
	syncRequest   int64
	StatusMap map[int64]*RequestStatus
}

type RequestStatus struct {
	RequestHash [16]byte
	Status	    string
}

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
		requestStat.RequestHash = requestobj.CheckSum
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

func (h *HttpServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//Go followes causually consistent memory model, so require sync among stat and normal request to get consistent stat data
	atomic.AddInt64(&h.Stat.syncRequest,int64(1))
	if (r.URL.Path == "/stat") && (h.NeedStats) {
                h.statLock.Lock()
		log.Trace(h.Stat)
		stat, err := json.MarshalIndent(h.Stat, "", " ")
                h.statLock.Unlock()
		if err != nil {
                        log.Error("(HTTP Server) Writing to http response writer failed :", err)
                }
		_, err = fmt.Fprintf(w, "%s", string(stat))
                if err != nil {
			log.Error("(HTTP Server) Writing to http response writer failed :", err)
                }
		return
	}

	//Blocks if more no specified no of request is already in the queue
	var thisRequestStat *RequestStatus
	var id int64

	if (h.NeedStats) {
		h.statLock.Lock()
		h.Stat.ReceivedCount += 1
		id = h.Stat.ReceivedCount
		h.Stat.Queued += 1
		thisRequestStat = &RequestStatus{
			Status : "Queued",
		}
		h.Stat.StatusMap[id] = thisRequestStat
		h.statLock.Unlock()
	}
	h.limiter <- 1
	defer func() {
		<-h.limiter
	}()

	var success bool
	switch r.Method {
	case "GET":
		fallthrough
	case "PUT":
		respString, err, state := h.process(r,thisRequestStat)
		success = state
		if err == nil {
			_, errRes := fmt.Fprintf(w, "%s", string(respString))
			if errRes != nil {
				log.Error("(HTTP Server) Writing to http response writer failed :", errRes)
			}
		} else {
			log.Error("(HTTP Server) Encoding or response obj failed:", err)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	if h.NeedStats{
		h.statLock.Lock()
		delete(h.Stat.StatusMap,id)
		h.Stat.FinishedCount += int64(1)
		switch r.Method{
		case "GET":
			h.Stat.GetCount += int64(1)
			if success{
				h.Stat.GetSuccessCount += int64(1)
			}
		case "PUT":
                        h.Stat.PutCount += int64(1)
                        if success{
                                h.Stat.PutSuccessCount += int64(1)
                        }
		}
		h.statLock.Unlock()
	}

}

//Blocking func
func (h *HttpServerHandler) StartServer() error {
	h.limiter = make(chan int, h.Limit)
	h.server = http.Server{}
	h.server.Addr = h.Addr + ":" + h.Port
	//Update the timeout using little's fourmula
	h.server.Handler = http.TimeoutHandler(h, 150*time.Second, "Server Timeout")
	h.Stat.StatusMap = make(map[int64]*RequestStatus)
	err := h.server.ListenAndServe()
	return err
}

//Close server
func (h HttpServerHandler) StopServer() error {
	err := h.server.Close()
	return err
}
