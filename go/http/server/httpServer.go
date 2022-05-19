package httpServer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type HTTPServerHandler struct {
	//Exported
	Addr                net.IP
	Port                string
	GETHandler          func([]byte, *[]byte) error
	PUTHandler          func([]byte) error
	HTTPConnectionLimit int
	PMDBServerConfig    map[string][]byte
	//Non-exported
	HTTPServer        http.Server
	rncui             string
	connectionLimiter chan int
	//For stats
	StatsRequired bool
	Stat          HTTPServerStat
	statLock      sync.Mutex
	Buckets		  int64
}

type HTTPServerStat struct {
	GetCount        		int64
	PutCount        		int64
	GetSuccessCount 		int64
	PutSuccessCount 		int64
	Queued          		int64
	ReceivedCount   		int64
	FinishedCount   		int64
	syncRequest     		int64
	receiveStats			opInfo
	sendStats				opInfo
	readStats				opInfo
	writeStats				opInfo
	ReceiveTimeMicroSec		map[string]int64
	ReceiveSize				map[string]int64
	SendTimeMicroSec		map[string]int64
	SendSize				map[string]int64
	ReadTimeMicroSec		map[string]int64
	ReadSize				map[string]int64
	WriteTimeMicroSec		map[string]int64
	WriteSize				map[string]int64
	StatusMap       		map[int64]*RequestStatus
}

type RequestStatus struct {
	RequestHash string
	Status      string
}

type opInfo struct{
	OpTimeMicroSec	[]int64
	RequestSize		[]int64
}

func (handler *HTTPServerHandler) configHandler(writer http.ResponseWriter, reader *http.Request) {

	uuid, err := ioutil.ReadAll(reader.Body)
	if err != nil {
		fmt.Fprintf(writer, "Unable to parse UUID")
	}
	configData, present := handler.PMDBServerConfig[string(uuid)]
	if present {
		fmt.Fprintf(writer, "%s", configData)
	} else {
		fmt.Fprintf(writer, "UUID not present")
	}
}

func (handler *HTTPServerHandler) statHandler(writer http.ResponseWriter, reader *http.Request) {
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

func (handler *HTTPServerHandler) updateStat(id int64, success bool, read bool) {
	handler.statLock.Lock()
	defer handler.statLock.Unlock()
	delete(handler.Stat.StatusMap, id)
	handler.Stat.FinishedCount += int64(1)
	if read {
		handler.Stat.GetCount += int64(1)
		if success {
			handler.Stat.GetSuccessCount += int64(1)
		}
	} else {
		handler.Stat.PutCount += int64(1)
		if success {
			handler.Stat.PutSuccessCount += int64(1)
		}
	}
}

func (handler *HTTPServerHandler) createStat(requestStatHandler *RequestStatus) int64 {
	handler.statLock.Lock()
	defer handler.statLock.Unlock()

	handler.Stat.ReceivedCount += 1
	id := handler.Stat.ReceivedCount
	handler.Stat.Queued += 1
	requestStatHandler = &RequestStatus{
		Status: "Queued",
	}
	handler.Stat.StatusMap[id] = requestStatHandler
	return id
}

func (handler *HTTPServerHandler) kvRequestHandler(writer http.ResponseWriter, reader *http.Request) {
	var thisRequestStat RequestStatus
	var id int64

	//Create stat for the request
	if handler.StatsRequired {
		id = handler.createStat(&thisRequestStat)
	}

	//HTTP connections limiter
	handler.connectionLimiter <- 1
	defer func() {
		<-handler.connectionLimiter
	}()

	var success bool
	var read bool
	var result []byte
	var err error
	//start timer
	reqTime:=time.Now()
	//Handle the KV request
	requestBytes, err := ioutil.ReadAll(reader.Body)
	//get read all time
	handler.timeTrack("REC",len(requestBytes),reqTime)

	opTime:=time.Now()
	switch reader.Method {
	case "GET":
		if handler.StatsRequired {
			thisRequestStat.Status = "Processing"
		}
		err = handler.GETHandler(requestBytes, &result)
		//get get time
		handler.timeTrack("GET",len(requestBytes),opTime)
		read = true
		fallthrough
	case "PUT":
		if !read {
			if handler.StatsRequired {
				thisRequestStat.Status = "Processing"
			}
			err = handler.PUTHandler(requestBytes)
			//get put time
			handler.timeTrack("PUT",len(requestBytes),opTime)
		}
		if err == nil {
			success = true
		}
		sendTime:=time.Now()
		fmt.Fprintf(writer, "%s", string(result))
		handler.timeTrack("SEN",len(requestBytes),sendTime)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}

	//Update status
	if handler.StatsRequired {
		handler.updateStat(id, success, read)
	}
}

//timer for operations
func (handler *HTTPServerHandler) timeTrack(op string, size int, timer time.Time){
	opTime := time.Since(timer)
	switch op {
		case "GET":
			handler.histPlace(handler.Stat.readStats.RequestSize,size)
			handler.histPlace(handler.Stat.readStats.OpTimeMicroSec,int(opTime.Microseconds()))
		case "PUT":
			handler.histPlace(handler.Stat.writeStats.RequestSize,size)
			handler.histPlace(handler.Stat.writeStats.OpTimeMicroSec,int(opTime.Microseconds()))
		case "REC":
			handler.histPlace(handler.Stat.receiveStats.RequestSize,size)
			handler.histPlace(handler.Stat.receiveStats.OpTimeMicroSec,int(opTime.Microseconds()))
		case "SEN":
			handler.histPlace(handler.Stat.sendStats.RequestSize,size)
			handler.histPlace(handler.Stat.sendStats.OpTimeMicroSec,int(opTime.Microseconds()))
		default:
		
	}
}

//increment count on correct position of histogram
func (handler *HTTPServerHandler) histPlace(h []int64, value int){
	v := 2
	added := false
	buckets:=handler.Buckets
	for i := 0; i < int(buckets)-1; i++ {
		if v >= value{
			h[i]++
			added=true
			break
		}
		v=v*2
	}
	if !added {
		h[buckets-1]++
	}
}

//create histogram with num of buckets and add number value to each bucket value
func (handler *HTTPServerHandler) makehist() []int64{
	buckets:=handler.Buckets
	h := make([]int64, buckets)
	return h
}

func (handler *HTTPServerHandler) makemap(ar []int64) map[string]int64{
	m := make(map[string]int64)
	v:=2
	for i := 0; i < len(handler.Stat.writeStats.OpTimeMicroSec)-1; i++ {
		zeroPad:=fmt.Sprintf("%08d",v)
		m[zeroPad]=ar[i]
		v=v*2
		if i== len(handler.Stat.writeStats.OpTimeMicroSec)-2{
			m[zeroPad+"+"]=ar[i+1]
		}
	}
	return m
}
//HTTP server handler called when request is received
func (handler *HTTPServerHandler) ServeHTTP(writer http.ResponseWriter, reader *http.Request) {
	//Go follows causually consistent memory model, so require sync among stat and normal request to get consistent stat data
	//atomic.AddInt64(&handler.Stat.syncRequest,int64(1))
	handler.Stat.ReceiveTimeMicroSec=handler.makemap(handler.Stat.receiveStats.OpTimeMicroSec)
	handler.Stat.ReceiveSize=handler.makemap(handler.Stat.receiveStats.RequestSize)
	handler.Stat.SendTimeMicroSec=handler.makemap(handler.Stat.sendStats.OpTimeMicroSec)
	handler.Stat.SendSize=handler.makemap(handler.Stat.sendStats.RequestSize)
	handler.Stat.ReadTimeMicroSec=handler.makemap(handler.Stat.readStats.OpTimeMicroSec)
	handler.Stat.ReadSize=handler.makemap(handler.Stat.readStats.RequestSize)
	handler.Stat.WriteTimeMicroSec=handler.makemap(handler.Stat.writeStats.OpTimeMicroSec)
	handler.Stat.WriteSize=handler.makemap(handler.Stat.writeStats.RequestSize)
	if reader.URL.Path == "/config" {
		handler.configHandler(writer, reader)
	} else if (reader.URL.Path == "/stat") && (handler.StatsRequired) {
		handler.statHandler(writer, reader)
	} else {
		handler.kvRequestHandler(writer, reader)
	}
}

//Start server
func (handler *HTTPServerHandler) Start_HTTPServer() error {
	handler.connectionLimiter = make(chan int, handler.HTTPConnectionLimit)
	handler.HTTPServer = http.Server{}
	handler.HTTPServer.Addr = handler.Addr.String() + ":" + handler.Port

	//make histograms
	handler.Stat.receiveStats.OpTimeMicroSec=handler.makehist()
	handler.Stat.receiveStats.RequestSize=handler.makehist()
	handler.Stat.sendStats.OpTimeMicroSec=handler.makehist()
	handler.Stat.sendStats.RequestSize=handler.makehist()
	handler.Stat.readStats.OpTimeMicroSec=handler.makehist()
	handler.Stat.readStats.RequestSize=handler.makehist()
	handler.Stat.writeStats.OpTimeMicroSec=handler.makehist()
	handler.Stat.writeStats.RequestSize=handler.makehist()

	//Update the timeout using little's fourmula
	handler.HTTPServer.Handler = http.TimeoutHandler(handler, 150*time.Second, "Server Timeout")
	handler.Stat.StatusMap = make(map[int64]*RequestStatus)

	//Start server
	err := handler.HTTPServer.ListenAndServe()
	return err
}

//Close server
func (h HTTPServerHandler) Stop_HTTPServer() error {
	err := h.HTTPServer.Close()
	return err
}
