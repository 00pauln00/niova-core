package main

//import _ "net/http/pprof"

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	//	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"github.com/hashicorp/serf/client"
	//	"github.com/hashicorp/serf/coordinate"
)

// #include <unistd.h>
// //#include <errno.h>
// //int usleep(useconds_t usec);
import "C"

func mlongsleep() {
	C.usleep(500000)
}

var (
	endpointRoot  *string // flag
	httpPort      *int    // flag
	showHelp      *bool   //flag
	showHelpShort *bool   //flag
)

func usage(rc int) {
	fmt.Printf("Usage: [OPTIONS] %s\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(rc)
}

func init() {
	endpointRoot =
		flag.String("dir", "/tmp/.niova", "endpoint directory root")

	httpPort = flag.Int("port", 8080, "http listen port")
	showHelpShort = flag.Bool("h", false, "")
	showHelp = flag.Bool("help", false, "print help")

	flag.Parse()

	nonParsed := flag.Args()
	if len(nonParsed) > 0 {
		fmt.Println("Unexpected argument found:", nonParsed[1])
		usage(1)
	}

	if *showHelpShort == true || *showHelp == true {
		usage(0)
	}
}

type serfConn struct {
	client   *client.RPCClient
	err      error
	lastConn time.Time
}

type epContainer struct {
	EpMap     map[uuid.UUID]*NcsiEP
	Mutex     sync.Mutex
	Path      string
	run       bool
	Statb     syscall.Stat_t
	EpWatcher *fsnotify.Watcher
	sconn     serfConn
}

func (epc *epContainer) tryAdd(uuid uuid.UUID) {
	lns := epc.EpMap[uuid]
	if lns == nil {
		newlns := NcsiEP{
			Uuid:         uuid,
			Path:         epc.Path + "/" + uuid.String(),
			Name:         "r-a4e1",
			NiovaSvcType: "raft",
			Port:         6666,
			LastReport:   time.Now(),
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

func (epc *epContainer) Scan() {
	files, err := ioutil.ReadDir(epc.Path)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		// Need to support removal of stale items
		if uuid, err := uuid.Parse(file.Name()); err == nil {
			epc.tryAdd(uuid)
		}
	}
}

func (epc *epContainer) Monitor() error {
	var err error = nil

	for epc.run == true {
		var tmp_stb syscall.Stat_t

		epc.sconn.serfConnect()

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
			ep.Detect()
		}

		// replace with inotify
		//		time.Sleep(500 * time.Millisecond)
		mlongsleep()
	}

	return err
}

func (epc *epContainer) JsonMarshalUUID(uuid uuid.UUID) []byte {
	var jsonData []byte
	var err error

	epc.Mutex.Lock()
	ep := epc.EpMap[uuid]
	if ep != nil {
		jsonData, err = json.MarshalIndent(epc.EpMap[uuid], "", "\t")
	} else {
		// Return an empty set if the item does not exist
		jsonData = []byte("{}") //XXx should we return an enoent err?
	}

	epc.Mutex.Unlock()

	if err != nil {
		return nil
	}

	return jsonData
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

func (epc *epContainer) processInotifyEvent(event *fsnotify.Event) {
	splitPath := strings.Split(event.Name, "/")

	templ := "ncsiep_"
	cmpstr := splitPath[len(splitPath)-1]

	if err := strings.Compare(cmpstr[0:len(templ)], templ); err != 0 {
		return
	}

	uuid, err := uuid.Parse(splitPath[len(splitPath)-3])
	if err != nil {
		return
	}

	if ep := epc.EpMap[uuid]; ep != nil {
		ep.Complete(cmpstr)
	}
}

func (epc *epContainer) epOutputWatcher() {
	for {
		select {
		case event := <-epc.EpWatcher.Events:
			//fmt.Printf("EVENT! %#v\n", event)

			if event.Op == fsnotify.Create {
				epc.processInotifyEvent(&event)
			}

			// watch for errors
		case err := <-epc.EpWatcher.Errors:
			fmt.Println("ERROR", err)
		}
	}
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

	epc.EpWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	epc.run = true

	go epc.epOutputWatcher()

	return nil
}

func (epc *epContainer) httpHandleRootRequest(w http.ResponseWriter) {
	fmt.Fprintf(w, "%s\n", string(epc.JsonMarshal()))
}

func (epc *epContainer) httpHandleUUIDRequest(w http.ResponseWriter,
	uuid uuid.UUID) {

	fmt.Fprintf(w, "%s\n", string(epc.JsonMarshalUUID(uuid)))
}

func (epc *epContainer) httpHandleRoute(w http.ResponseWriter, r *url.URL) {
	// Splitting '/vX/' results in a length of 2
	splitURL := strings.Split(r.String(), "/v0/")

	//log.Printf("%+v (url-path-len=%d str=%s)\n", splitURL, len(splitURL),
	//	r.String())

	// Root level request
	if len(splitURL) == 2 && len(splitURL[1]) == 0 {
		epc.httpHandleRootRequest(w)
		return
	}

	if uuid, err := uuid.Parse(splitURL[1]); err == nil {
		epc.httpHandleUUIDRequest(w, uuid)

	} else {
		fmt.Fprintln(w, "Invalid request: url", splitURL[1])
	}

}

func (epc *epContainer) HttpHandle(w http.ResponseWriter, r *http.Request) {
	epc.httpHandleRoute(w, r.URL)
}

func (epc *epContainer) serveHttp() {
	http.HandleFunc("/v0/", epc.HttpHandle)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*httpPort), nil))
}

func (sconn *serfConn) serfConnect() {
	if sconn.client != nil {
		if sconn.err == nil {
			return
		} else {
			sconn.client.Close()
			sconn.client = nil
		}
	}

	serfConf := client.Config{Addr: "127.0.0.1:7373"}
	//	serfCli := client.RPCClient

	sconn.client, sconn.err = client.ClientFromConfig(&serfConf)
	if sconn.err != nil {
		log.Println("client.ClientFromConfig():", sconn.err)
		return
	} else {
		sconn.lastConn = time.Now()
	}

	members, err := sconn.client.Members()
	log.Printf("%+v err=%s\n", members, err)
	mlongsleep()
}

func main() {
	var epc epContainer

	if err := epc.Init(*endpointRoot); err != nil {
		log.Fatalf("epc.Init('%s'): %s", *endpointRoot, err)
	}

	epc.Scan()

	go epc.serveHttp()

	epc.Monitor()
}
