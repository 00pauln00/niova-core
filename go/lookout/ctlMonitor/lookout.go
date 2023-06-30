package lookout

import (
	"bytes"
	"common/prometheus_handler"
	"common/requestResponseLib"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

var HttpPort int

type EPContainer struct {
	MonitorUUID      string
	CTLPath          string
	PromPath         string
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
	PortRange        []uint16
	PromPort         int
	RetPort          *int
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

func (epc *EPContainer) CheckLiveness(peerUuid string) bool {
	uuid, _ := uuid.Parse(peerUuid)
	for {
		if epc.EpMap[uuid] == nil {
			time.Sleep(1 * time.Second)
		} else {
			return epc.EpMap[uuid].Alive
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

		//TODO Change env variable to LOOKOUT SLEEP
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
	ep.CtlCustomQuery(query, httpID)

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

func loadSystemInfo(labelMap map[string]string, sysInfo SystemInfo) map[string]string {
	labelMap["NODE_NAME"] = sysInfo.UtsNodename
	labelMap["SYS_NAME"] = sysInfo.UtsSysname
	labelMap["MACHINE"] = sysInfo.UtsMachine

	return labelMap
}

func loadPMDBLabelMap(labelMap map[string]string, raftEntry RaftInfo) map[string]string {
	labelMap["STATE"] = raftEntry.State
	labelMap["RAFT_UUID"] = raftEntry.RaftUUID
	labelMap["VOTED_FOR"] = raftEntry.VotedForUUID
	labelMap["FOLLOWER_REASON"] = raftEntry.FollowerReason
	labelMap["CLIENT_REQS"] = raftEntry.ClientRequests

	return labelMap
}

func loadNISDLabelMap(labelMap map[string]string, nisdRootEntry NISDRoot) map[string]string {
	labelMap["STATUS"] = nisdRootEntry.Status
	labelMap["ALT_NAME"] = nisdRootEntry.AltName

	return labelMap
}

func getFollowerStats(raftEntry RaftInfo) string {
	var output string
	for indx := range raftEntry.FollowerStats {
		UUID := raftEntry.FollowerStats[indx].PeerUUID
		NextIdx := raftEntry.FollowerStats[indx].NextIdx
		PrevIdxTerm := raftEntry.FollowerStats[indx].PrevIdxTerm
		LastAckMs := raftEntry.FollowerStats[indx].LastAckMs
		output += "\n" + fmt.Sprintf(`follower_stats{uuid="%s"next_idx="%d"prev_idx_term="%d"}%d`, UUID, NextIdx, PrevIdxTerm, LastAckMs)
	}
	return output
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
		} else {
			// since we do not know the state of other nodes
			output += "\n" + fmt.Sprintf(`node_status{uuid="%s"status="%s"raftUUID="%s"} %s`, name, status, raftUUID, adder)
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
		labelMap = loadSystemInfo(labelMap, node.EPInfo.SysInfo)
	}

	if epc.AppType == "PMDB" {
		parsedUUID, _ := uuid.Parse(epc.MonitorUUID)
		node := nodeMap[parsedUUID]
		labelMap["PMDB_UUID"] = parsedUUID.String()
		labelMap["TYPE"] = epc.AppType
		// Loading labelMap with PMDB data
		labelMap = loadPMDBLabelMap(labelMap, node.EPInfo.RaftRootEntry[0])
		// Parsing exported data
		output += prometheus_handler.GenericPromDataParser(node.EPInfo.RaftRootEntry[0], labelMap)
		// Parsing membership data
		output += epc.parseMembershipPrometheus(node.EPInfo.RaftRootEntry[0].State, node.EPInfo.RaftRootEntry[0].RaftUUID, parsedUUID.String())
		// Parsing follower data
		output += getFollowerStats(node.EPInfo.RaftRootEntry[0])
		// Parsing system info
		output += prometheus_handler.GenericPromDataParser(node.EPInfo.SysInfo, labelMap)
	} else if epc.AppType == "NISD" {
		for uuid, node := range nodeMap {
			labelMap["NISD_UUID"] = uuid.String()
			labelMap["TYPE"] = epc.AppType
			labelMap = loadNISDLabelMap(labelMap, node.EPInfo.NISDRootEntry[0])
			// Parse NISDInfo
			output += prometheus_handler.GenericPromDataParser(node.EPInfo.NISDInformation[0], labelMap)
			// Parse NISDRootEntry
			output += prometheus_handler.GenericPromDataParser(node.EPInfo.NISDRootEntry[0], labelMap)
			// Parse nisd system info
			output += prometheus_handler.GenericPromDataParser(node.EPInfo.SysInfo, labelMap)
		}
	}
	fmt.Fprintln(w, output)
}

func (epc *EPContainer) serveHttp() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/", epc.QueryHandle)
	mux.HandleFunc("/v0/", epc.HttpHandle)
	mux.HandleFunc("/metrics", epc.MetricsHandler)
	for i := len(epc.PortRange) - 1; i >= 0; i-- {
		epc.HttpPort = int(epc.PortRange[i])
		l, err := net.Listen("tcp", ":"+strconv.Itoa(epc.HttpPort))
		if err != nil {
			if strings.Contains(err.Error(), "bind") {
				continue
			} else {
				fmt.Println("Error while starting lookout - ", err)
				return err
			}
		} else {
			go func() {
				*epc.RetPort = epc.HttpPort
				fmt.Println("Serving at - ", epc.HttpPort)
				http.Serve(l, mux)
			}()
		}
		break
	}
	return nil
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

func (epc *EPContainer) writePromPath() error {
	f, err := os.OpenFile(epc.PromPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(strconv.Itoa(epc.HttpPort))
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}

	return nil
}

func (epc *EPContainer) Start() error {
	var err error
	errs := make(chan error, 1)
	//Start http service
	if epc.EnableHttp {
		epc.httpQuery = make(map[string](chan []byte))
		go func() {
			err_r := epc.serveHttp()
			errs <- err_r
			if <-errs != nil {
				return
			}
		}()
		if err := <-errs; err != nil {
			*epc.RetPort = -1
			return err
		}
	}

	fmt.Println("============================")
	fmt.Println(epc.HttpPort)
	fmt.Println("============================")
	err = epc.writePromPath()
	if err != nil {
		return err
	}
	//Setup lookout
	err = epc.init()
	if err != nil {
		log.Printf("Lookout Init - ", err)
		return err
	}

	//Start monitoring
	err = epc.monitor()
	if err != nil {
		log.Printf("Lookout Monitor - ", err)
		return err
	}

	return nil
}
