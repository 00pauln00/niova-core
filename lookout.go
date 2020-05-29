package main

import (
	"fmt"
	"log"
	"net/http"
	//	"net/url"
	"syscall"
	//	"strings"
	"sync"
	"time"

	"io/ioutil"
	"encoding/json"
	"github.com/google/uuid"
)

type ctlsvcEP struct {
	Uuid         uuid.UUID `json:"-"`    // Field is ignored by json
	Path         string    `json:"-"`
	Name         string    `json:"name"`
	NiovaSvcType string    `json:"type"`
	Port         int       `json:"port"`
	LastReport   time.Time `json:"-"`
	Alive        bool      `json:"responsive"`
}

type epContainer struct {
	EpMap   map[uuid.UUID]*ctlsvcEP
	Mutex   sync.Mutex
	Path    string
	run     bool
	Statb   syscall.Stat_t
}


//func (ep *ctlsvcEP) Update() int {
//	err, sys_info := CtlSvcSysInfoQuery(ncsEpDir, ep.Uuid)
//	return err
//}

func (epc *epContainer) tryAdd(uuid uuid.UUID) {
	lns := epc.EpMap[uuid]
	if lns == nil {
		newlns := ctlsvcEP {
			uuid, epc.Path + "/" + uuid.String(), "r-a4e1",
				"raft", 6666, time.Now(), true,
			}

		// serialize with readers in httpd context, this is the only
		// writer thread so the lookup above does not require a lock
		epc.Mutex.Lock()
		epc.EpMap[uuid] = &newlns
		epc.Mutex.Unlock()
		log.Printf("added: %+v\n", newlns)
	}
}

func (epc *epContainer) Scan() {
	files, err := ioutil.ReadDir(epc.Path)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		// Need to support removal of stale items
		uuid, err := uuid.Parse(file.Name())

		if err == nil {
			epc.tryAdd(uuid)
		}
	}
}

func (epc *epContainer) Monitor() error {
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
			epc.Scan()
		}

		// Query for liveness
		//		for _, ep := range ncsEPs {
		//		ep.Update()
		//}

		// replace with inotify
 		time.Sleep(500 * time.Millisecond)
	}

	return err
}

func (epc *epContainer) JsonMarshal() []byte {
	var jsonData []byte

	epc.Mutex.Lock()
	jsonData, err := json.MarshalIndent(epc.EpMap, "", "\t")
	epc.Mutex.Unlock()

	if err != nil {
		return nil
	}

	return jsonData
}

func (epc *epContainer) Init(path string) error {
	// Check the provided endpoint root path
	err := syscall.Stat(path, &epc.Statb)
	if err != nil {
		return err
	}

	// Set path (Xxx still need to check if this is a directory or not)
	epc.Path = path

	// Create the map
	epc.EpMap = make(map[uuid.UUID]*ctlsvcEP)
	if epc.EpMap == nil {
		return syscall.ENOMEM
	}

	epc.run = true

	return nil
}

func (epc *epContainer) HttpHandle(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%s\n", string(epc.JsonMarshal()))
}

func main() {
	var epc epContainer

	err := epc.Init("/tmp/.niova")
	if err != nil {
		log.Fatal("epc.Init(): %d", err)
	}

	epc.Scan()

	// monitor in another thread
	go epc.Monitor()

	http.HandleFunc("/v0/", epc.HttpHandle)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
