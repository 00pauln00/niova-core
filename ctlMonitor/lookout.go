package lookout

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net/http"
	"common/requestResponseLib"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"common/prometheus_handler"
	"time"
)

type EPContainer struct {
	MonitorUUID   string
	EpMap         map[uuid.UUID]*NcsiEP
	Mutex         sync.Mutex
	Path          string
	run           bool
	Statb         syscall.Stat_t
	EpWatcher     *fsnotify.Watcher
	httpQuery     map[string](chan []byte)
	AppType	      string
	HttpPort      int
}


func (epc *EPContainer) tryAdd(uuid uuid.UUID) {
	lns := epc.EpMap[uuid]
	if lns == nil {
		newlns := NcsiEP{
			Uuid:         uuid,
			Path:         epc.Path + "/" + uuid.String(),
			Name:         "r-a4e1",
			NiovaSvcType: "raft",
			Port:         6666,
			LastReport:   time.Now(),
			LastClear:    time.Now(),
			Alive:        true,
			pendingCmds:  make(map[string]*epCommand),
		}

		if err := epc.EpWatcher.Add(newlns.Path + "/output"); err != nil {
			log.Fatalln("Watcher.Add() failed:", err)
		}

		// serialize with readers in httpd context, this is the only
		// writer thread so the lookup above does not require a lock
		epc.Mutex.Lock()
		epc.EpMap[uuid] = &newlns
		epc.Mutex.Unlock()
		log.Printf("added: %+v\n", newlns)
	}
}

func (epc *EPContainer) scan() {
	files, err := ioutil.ReadDir(epc.Path)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		// Need to support removal of stale items
		if uuid, err := uuid.Parse(file.Name()); err == nil {
			if ((epc.MonitorUUID == uuid.String()) || (epc.MonitorUUID == "*")) {
				epc.tryAdd(uuid)
			}
		}
	}
}

func (epc *EPContainer) monitor() error {
	var err error = nil

	for epc.run == true {
		var tmp_stb syscall.Stat_t

		err = syscall.Stat(epc.Path, &tmp_stb)
		if err != nil {
			log.Printf("syscall.Stat('%s'): %s", epc.Path, err)
			break
		}

		if tmp_stb.Mtim != epc.Statb.Mtim {
			epc.Statb = tmp_stb
			epc.scan()
		}

		// Query for liveness
		for _, ep := range epc.EpMap {
			ep.Remove()
			ep.Detect(epc.AppType)
		}

		time.Sleep(time.Second)
	}

	return err
}

func (epc *EPContainer) JsonMarshalUUID(uuid uuid.UUID) []byte {
	var jsonData []byte
	var err error

	epc.Mutex.Lock()
	ep := epc.EpMap[uuid]
	epc.Mutex.Unlock()
	if ep != nil {
		jsonData, err = json.MarshalIndent(ep, "", "\t")
	} else {
		// Return an empty set if the item does not exist
		jsonData = []byte("{}")
	}

	if err != nil {
		return nil
	}

	return jsonData
}

func (epc *EPContainer) JsonMarshal() []byte {
	var jsonData []byte

	epc.Mutex.Lock()
	jsonData, err := json.MarshalIndent(epc.EpMap, "", "\t")
	epc.Mutex.Unlock()

	if err != nil {
		return nil
	}

	return jsonData
}

func (epc *EPContainer) processInotifyEvent(event *fsnotify.Event) {
	splitPath := strings.Split(event.Name, "/")
	cmpstr := splitPath[len(splitPath)-1]
	uuid, err := uuid.Parse(splitPath[len(splitPath)-3])
        if err != nil {
                return
        }

	//temp file exclusion
	if strings.Contains(cmpstr,".") {
		return
        }

	//Check if its for HTTP
	if strings.Contains(cmpstr, "HTTP") {
		var output []byte
		if ep := epc.EpMap[uuid]; ep != nil {
			err := ep.Complete(cmpstr, &output)
			if err != nil {
				output = []byte(err.Error())
			}
		}

		if channel, ok := epc.httpQuery[cmpstr]; ok {
			channel <- output
		}
		return
	}

	if ep := epc.EpMap[uuid]; ep != nil {
		ep.Complete(cmpstr, nil)
	}
}

func (epc *EPContainer) epOutputWatcher() {
	for {
		select {
		case event := <-epc.EpWatcher.Events:

			if event.Op == fsnotify.Create {
				epc.processInotifyEvent(&event)
			}

			// watch for errors
		case err := <-epc.EpWatcher.Errors:
			fmt.Println("ERROR", err)

		}
	}
}

func (epc *EPContainer) init() error {
	// Check the provided endpoint root path
	err := syscall.Stat(epc.Path, &epc.Statb)
	if err != nil {
		return err
	}

	// Set path (Xxx still need to check if this is a directory or not)

	// Create the map
	epc.EpMap = make(map[uuid.UUID]*NcsiEP)
	if epc.EpMap == nil {
		return syscall.ENOMEM
	}

	epc.EpWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	epc.run = true

	go epc.epOutputWatcher()

	return nil
}

func (epc *EPContainer) httpHandleRootRequest(w http.ResponseWriter) {
	fmt.Fprintf(w, "%s\n", string(epc.JsonMarshal()))
}

func (epc *EPContainer) httpHandleUUIDRequest(w http.ResponseWriter,
	uuid uuid.UUID) {

	fmt.Fprintf(w, "%s\n", string(epc.JsonMarshalUUID(uuid)))
}

func (epc *EPContainer) httpHandleRoute(w http.ResponseWriter, r *url.URL) {
	splitURL := strings.Split(r.String(), "/v0/")


	if len(splitURL) == 2 && len(splitURL[1]) == 0 {
		epc.httpHandleRootRequest(w)

	} else if uuid, err := uuid.Parse(splitURL[1]); err == nil {
		epc.httpHandleUUIDRequest(w, uuid)

	} else {
		fmt.Fprintln(w, "Invalid request: url", splitURL[1])
	}

}

func (epc *EPContainer) HttpHandle(w http.ResponseWriter, r *http.Request) {
	epc.httpHandleRoute(w, r.URL)
}


func (epc *EPContainer) customQuery(node uuid.UUID, query string) []byte {
	epc.Mutex.Lock()
        ep := epc.EpMap[node]
        epc.Mutex.Unlock()

	//If not present
	if ep == nil{
		return []byte("Specified NISD is not present")
	}

	httpID := "HTTP_"+uuid.New().String()
	epc.httpQuery[httpID] = make(chan []byte, 2)
	ep.CustomQuery(query, httpID)

	//FIXME: Have select in case of NISD dead
	var byteOP []byte
	select {
	case byteOP = <- epc.httpQuery[httpID]:
		break
	}
	return byteOP
}

func (epc *EPContainer) QueryHandle(w http.ResponseWriter, r *http.Request) {

	//Decode the NISD request structure
	requestBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}

	requestObj := requestResponseLib.LookoutRequest{}
        dec := gob.NewDecoder(bytes.NewBuffer(requestBytes))
        err = dec.Decode(&requestObj)
        if err != nil {
                log.Println(err)
        }

	//Call the appropriate function
	output := epc.customQuery(requestObj.UUID, requestObj.Cmd)

	//Data to writer
	w.Write(output)
}

func (epc *EPContainer) MetricsHandler(w http.ResponseWriter, r *http.Request) {
        //Split key based nisd's UUID and field
        var output string
        parsedUUID, _ := uuid.Parse(epc.MonitorUUID)
        node := epc.EpMap[parsedUUID]
                if len(node.EPInfo.RaftRootEntry) > 0 {
                        output += prometheus_handler.GenericPromDataParser(node.EPInfo.RaftRootEntry[0])
                }

        fmt.Fprintln(w, output)
}

func (epc *EPContainer) serveHttp() {
	http.HandleFunc("/v1/", epc.QueryHandle)
	http.HandleFunc("/v0/", epc.HttpHandle)
	http.HandleFunc("/metrics", epc.MetricsHandler)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(epc.HttpPort), nil))
}

func (epc *EPContainer) Start() {
	//Start http service
	go epc.serveHttp()

	//Setup lookout
	epc.init()

	//Start monitoring
	epc.monitor()
}
