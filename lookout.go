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

	"encoding/json"
	"github.com/google/uuid"
	"io/ioutil"
)

type epContainer struct {
	EpMap map[uuid.UUID]*NcsiEP
	Mutex sync.Mutex
	Path  string
	run   bool
	Statb syscall.Stat_t
}

func (epc *epContainer) tryAdd(uuid uuid.UUID) {
	lns := epc.EpMap[uuid]
	if lns == nil {
		newlns := NcsiEP{
			Uuid: uuid, Path: epc.Path + "/" + uuid.String(),
			Name: "r-a4e1", NiovaSvcType: "raft", Port: 6666,
			LastReport: time.Now(), Alive: true,
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
		for _, ep := range epc.EpMap {
			go ep.Detect()
		}

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
	epc.EpMap = make(map[uuid.UUID]*NcsiEP)
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
