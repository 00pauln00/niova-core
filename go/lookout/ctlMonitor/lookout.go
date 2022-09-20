package lookout

import (
	"bytes"
	"common/prometheus_handler"
	"common/requestResponseLib"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type EPContainer struct {
	MonitorUUID      string
	CTLPath          string
	AppType          string
	HttpPort         int
	EnableHttp       bool
	SerfMembershipCB func() map[string]bool
	Statb            syscall.Stat_t
	EpWatcher        *fsnotify.Watcher
	EpMap            map[uuid.UUID]*NcsiEP
	mutex            sync.Mutex
	run              bool
	httpQuery        map[string](chan []byte)
}

func (epc *EPContainer) tryAdd(uuid uuid.UUID) {
	lns := epc.EpMap[uuid]
	if lns == nil {
		//FIXME: Do we need Port
		newlns := NcsiEP{
			Uuid:         uuid,
			Path:         epc.CTLPath + "/" + uuid.String(),
			Name:         "r-a4e1",
			NiovaSvcType: "raft",
			Port:         6666,
			LastReport:   time.Now(),
			LastClear:    time.Now(),
			Alive:        true,
			pendingCmds:  make(map[string]*epCommand),
		}

		if err := epc.EpWatcher.Add(newlns.Path + "/output"); err != nil {
			log.Fatal("Watcher.Add() failed:", err)
		}

		// serialize with readers in httpd context, this is the only
		// writer thread so the lookup above does not require a lock
		epc.mutex.Lock()
		epc.EpMap[uuid] = &newlns
		epc.mutex.Unlock()
		log.Printf("added: %+v\n", newlns)
	}
}

func (epc *EPContainer) scan() {
	files, err := ioutil.ReadDir(epc.CTLPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		// Need to support removal of stale items
		if uuid, err := uuid.Parse(file.Name()); err == nil {
			if (epc.MonitorUUID == uuid.String()) || (epc.MonitorUUID == "*") {
				epc.tryAdd(uuid)
			}
		}
	}
}

func (epc *EPContainer) monitor() error {
	var err error = nil

	for epc.run == true {
		var tmp_stb syscall.Stat_t
		var sleepTime time.Duration
		err = syscall.Stat(epc.CTLPath, &tmp_stb)
		if err != nil {
			log.Printf("syscall.Stat('%s'): %s", epc.CTLPath, err)
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

		sleepTimeStr := os.Getenv("NISD_LOOKOUT_SLEEP")
		sleepTime, err = time.ParseDuration(sleepTimeStr)
		if err != nil {
			sleepTime = 5
			log.Printf("Bad environment variable - Defaulting to standard value")
		}
		time.Sleep((sleepTime) * time.Second)
	}

	return err
}

func (epc *EPContainer) JsonMarshalUUID(uuid uuid.UUID) []byte {
	var jsonData []byte
	var err error

	epc.mutex.Lock()
	ep := epc.EpMap[uuid]
	epc.mutex.Unlock()
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

	epc.mutex.Lock()
	jsonData, err := json.MarshalIndent(epc.EpMap, "", "\t")
	epc.mutex.Unlock()

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
	if strings.Contains(cmpstr, ".") {
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
		err := ep.Complete(cmpstr, nil)
		if err != nil {
			fmt.Println(err)
		}
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
	err := syscall.Stat(epc.CTLPath, &epc.Statb)
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

	epc.scan()

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
	epc.mutex.Lock()
	ep := epc.EpMap[node]
	epc.mutex.Unlock()

	//If not present
	if ep == nil {
		return []byte("Specified NISD is not present")
	}

	httpID := "HTTP_" + uuid.New().String()
	epc.httpQuery[httpID] = make(chan []byte, 2)
	ep.CustomQuery(query, httpID)

	//FIXME: Have select in case of NISD dead and delete the channel
	var byteOP []byte
	select {
	case byteOP = <-epc.httpQuery[httpID]:
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

func (epc *EPContainer) parseMembershipPrometheus(state string, raftUUID string, nodeUUID string) string {
	var output string
	membership := epc.SerfMembershipCB()
	for name, isAlive := range membership {
		var adder, status string
		if isAlive {
			adder = "1"
			status = "online"
		} else {
			adder = "0"
			status = "offline"
		}
		if nodeUUID == name {
			output += "\n" + fmt.Sprintf(`node_status{uuid="%s"state="%s"status="%s"raftUUID="%s"} %s`, name, state, status, raftUUID, adder)
		}
	}
	return output
}

func (epc *EPContainer) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	//Split key based nisd's UUID and field
	var output string

	//Take snapshot of the EpMap
	nodeMap := make(map[uuid.UUID]*NcsiEP)
	epc.mutex.Lock()
	for k, v := range epc.EpMap {
		nodeMap[k] = v
	}
	epc.mutex.Unlock()
	labelMap := make(map[string]string)
	for _, node := range nodeMap {
		labelMap["NODE_NAME"] = node.EPInfo.SysInfo.UtsNodename
		labelMap["SYS_NAME"]= node.EPInfo.SysInfo.UtsSysname
		labelMap["MACHINE"]= node.EPInfo.SysInfo.UtsMachine
	}
	if epc.AppType == "PMDB" {
		parsedUUID, _ := uuid.Parse(epc.MonitorUUID)
		node := nodeMap[parsedUUID]
		labelMap["PMDB_UUID"] = parsedUUID.String()
		labelMap["STATE"] = node.EPInfo.RaftRootEntry[0].State
		labelMap["RAFT_UUID"] = node.EPInfo.RaftRootEntry[0].RaftUUID
		labelMap["TYPE"] = epc.AppType
		labelMap["VOTED_FOR"] = node.EPInfo.RaftRootEntry[0].VotedForUUID
		labelMap["FOLLOWER_REASON"] = node.EPInfo.RaftRootEntry[0].FollowerReason
		labelMap["CLIENT_REQS"] = node.EPInfo.RaftRootEntry[0].ClientRequests
		output += prometheus_handler.GenericPromDataParser(node.EPInfo.RaftRootEntry[0], labelMap)
		output += epc.parseMembershipPrometheus(node.EPInfo.RaftRootEntry[0].State, node.EPInfo.RaftRootEntry[0].RaftUUID, parsedUUID.String())
		output += prometheus_handler.GenericPromDataParser(node.EPInfo.SysInfo, labelMap)
	} else if epc.AppType == "NISD" {
		for uuid, node := range nodeMap {
			labelMap["NISD_UUID"] = uuid.String()
			labelMap["STATUS"] = node.EPInfo.NISDRootEntry[0].Status
			labelMap["ALT_NAME"] = node.EPInfo.NISDRootEntry[0].AltName
			labelMap["TYPE"] = epc.AppType
			output += prometheus_handler.GenericPromDataParser(node.EPInfo.NISDInformation[0], labelMap)
			output += prometheus_handler.GenericPromDataParser(node.EPInfo.NISDRootEntry[0], labelMap)
			output += prometheus_handler.GenericPromDataParser(node.EPInfo.SysInfo, labelMap)
		}
	}
	fmt.Fprintln(w, output)
}

func (epc *EPContainer) serveHttp() {
	http.HandleFunc("/v1/", epc.QueryHandle)
	http.HandleFunc("/v0/", epc.HttpHandle)
	http.HandleFunc("/metrics", epc.MetricsHandler)
	err := http.ListenAndServe(":"+strconv.Itoa(epc.HttpPort), nil)
	if err != nil {
		fmt.Println(err)
	}
}

func (epc *EPContainer) GetList() map[uuid.UUID]*NcsiEP {
	epc.mutex.Lock()
	defer epc.mutex.Unlock()
	return epc.EpMap
}

func (epc *EPContainer) MarkAlive(serviceUUID string) error {
	serviceID, err := uuid.Parse(serviceUUID)
	if err != nil {
		return err
	}
	service, ok := epc.EpMap[serviceID]
	if ok && service.Alive {
		service.pendingCmds = make(map[string]*epCommand)
		service.Alive = true
		service.LastReport = time.Now()
	}
	return nil
}

func (epc *EPContainer) Start() {
	//Start http service
	if epc.EnableHttp {
		go epc.serveHttp()
	}

	//Setup lookout
	epc.init()

	//Start monitoring
	epc.monitor()
}
