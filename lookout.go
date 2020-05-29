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
	Alive        bool      `json:responsive"`
}

type epContainer struct {
	EpMap map[uuid.UUID]*ctlsvcEP
	Mutex sync.Mutex
	Path string
	Monitor bool
}


var (
	ncsEPs = make(map[uuid.UUID]*ctlsvcEP)
	ncsEpMutex sync.Mutex
	ncsEpCont bool = true
	ncsEpDir string = "/tmp/.niova"
)



func ctlsvcScan(m map[uuid.UUID]*ctlsvcEP) {
	files, err := ioutil.ReadDir(ncsEpDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		// Need to support removal of stale items
		uuid, err := uuid.Parse(file.Name())

		if err != nil {
			continue
		}

		lns := m[uuid]
		if lns == nil {
			newlns := ctlsvcEP {
				uuid, ncsEpDir + "/" + file.Name(), "r-a4e1",
				"raft", 6666, time.Now(), true,
			}
			ncsEpMutex.Lock()
			m[uuid] = &newlns
			ncsEpMutex.Unlock()

			log.Printf("added: %+v\n", newlns)
		}
	}
}

func (ep *ctlsvcEP) Update() int {
	err, sys_info := CtlSvcSysInfoQuery(ncsEpDir, ep.Uuid)
	return err
}

func niovaCtlSvcMonitor() {
	var stb syscall.Stat_t // niovaSvcDetect() only when dir mtime changes

	for ncsEpCont == true {
		var tmp_stb syscall.Stat_t
		err := syscall.Stat(ncsEpDir, &tmp_stb)
		if err != nil {
			log.Printf("syscall.Stat('%s'): %s", ncsEpDir, err)
			break
		}

		if tmp_stb.Mtim != stb.Mtim {
			stb = tmp_stb
			niovaCtlSvcScan(ncsEPs)
		}

		// Query for liveness
		for _, ep := range ncsEPs {
			ep.Update()
		}

		// replace with inotify
 		time.Sleep(500 * time.Millisecond)
	}
}

//func niovaURLParse(url *url.URL) {
//	log.Print(url)
//	urlcomponents := strings.Split(url.String(), "/")
//
//	log.Print(urlcomponents)
//}

func niovaHandlerMap(w http.ResponseWriter, r *http.Request) {
	var jsonData []byte

	ncsEpMutex.Lock()
	jsonData, err := json.MarshalIndent(ncsEPs, "", "\t")
	ncsEpMutex.Unlock()

	if err != nil {
		log.Println(err)
	}
	fmt.Fprintf(w, "%s\n", string(jsonData))
}

func main() {
	var eps epContainer


	niovaCtlSvcScan(ncsEPs)

	go niovaCtlSvcMonitor()

	http.HandleFunc("/v0/", niovaHandlerMap)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
