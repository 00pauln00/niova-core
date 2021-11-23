package main

import (
	"errors"
	"flag"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"niovakv/niovakvlib"
	"pmdbServer/serfagenthandler"
	"os"
	"unsafe"
	"io/ioutil"
	"bufio"
	"strings"
	"strconv"
	defaultLogger "log"
	log "github.com/sirupsen/logrus"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0
// Use the default column family
var colmfamily = "PMDBTS_CF"

type pmdbServerHandler struct{
	raftUUID string
        peerUUID string
        logDir string
        logLevel string
        aport string
	configData map[string]string
}


func main() {
	serverHandler := pmdbServerHandler{}
	nso, pErr := serverHandler.parseArgs()
	if pErr != nil {
		log.Println(pErr)
		return
	}

	switch serverHandler.logLevel{
		case "Info":
			log.SetLevel(log.InfoLevel)
		case "Trace":
			log.SetLevel(log.TraceLevel)
	}

	//Create log file
	err := PumiceDBCommon.InitLogger(serverHandler.logDir)
	if err != nil {
		log.Error("Error while initating logger ", err)
		os.Exit(1)
	}

	err = serverHandler.startSerfAgent()
	if err != nil {
		log.Fatal("Error while initializing serf agent ",err)
	}

	log.Info("Raft and Peer UUID: ", nso.raftUuid, " ", nso.peerUuid)
	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	nso.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: colmfamily,
		RaftUuid:       nso.raftUuid,
		PeerUuid:       nso.peerUuid,
		PmdbAPI:        nso,
                SyncWrites:     false,
                CoalescedWrite: true,
	}

	// Start the pmdb server
	err = nso.pso.Run()

	if err != nil {
		log.Error(err)
	}
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func (handler *pmdbServerHandler) parseArgs() (*NiovaKVServer, error) {

	var err error

	flag.StringVar(&handler.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&handler.peerUUID, "u", "NULL", "peer uuid")

	/* If log path is not provided, it will use Default log path.
           default log path: /tmp/<peer-uuid>.log
        */
	defaultLog := "/" + "tmp" + "/" + handler.peerUUID + ".log"
	flag.StringVar(&handler.logDir, "l", defaultLog, "log dir")
	flag.StringVar(&handler.logLevel,"ll","Info","Log level")
	flag.StringVar(&handler.aport,"p","NULL","Serf agent port")
	flag.Parse()

	nso := &NiovaKVServer{}
	nso.raftUuid = handler.raftUUID
	nso.peerUuid = handler.peerUUID

	if nso == nil {
		err = errors.New("Not able to parse the arguments")
	} else {
		err = nil
	}

	return nso, err
}

func (handler *pmdbServerHandler) readConfig() {
	folder := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	file := folder+"/"+handler.peerUUID+".peer"
	f,_ := os.Open(file)
        scanner := bufio.NewScanner(f)
        handler.configData = make(map[string]string)

	for scanner.Scan() {
                        text := scanner.Text()
                        lastIndex := len(strings.Split(text," "))-1
                        key := strings.Split(text," ")[0]
                        value := strings.Split(text," ")[lastIndex]
                        handler.configData[key] = value
         }
}

func  (handler *pmdbServerHandler) startSerfAgent() error {

	serfLog := "00"

	switch serfLog{
        case "ignore":
                defaultLogger.SetOutput(ioutil.Discard)
        default:
                f, err := os.OpenFile("serfLog.log", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
                if err != nil {
                        defaultLogger.SetOutput(os.Stderr)
                } else {
                        defaultLogger.SetOutput(f)
                }
	}

        //defaultLogger.SetOutput(ioutil.Discard)
	serfAgentHandler := serfagenthandler.SerfAgentHandler{
		Name: handler.peerUUID,
		BindAddr: "127.0.0.1",
	}
        serfAgentHandler.BindPort, _ = strconv.Atoi(handler.aport)
        serfAgentHandler.AgentLogger = defaultLogger.Default()
	//Start serf agent
        agentJoinAddrs := []string{"127.0.0.1:8505","127.0.0.1:8501","127.0.0.1:8502","127.0.0.1:8503","127.0.0.1:8504"}
	_, err := serfAgentHandler.Startup(agentJoinAddrs, true)

	handler.readConfig()
        handler.configData["Type"] = "PMDB_SERVER"
        serfAgentHandler.SetTags(handler.configData)

	return err
}



type NiovaKVServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

func (nso *NiovaKVServer) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHandle unsafe.Pointer) {

	log.Trace("NiovaCtlPlane server: Apply request received")

	// Decode the input buffer into structure format
	applyNiovaKV := &niovakvlib.NiovaKV{}

	decodeErr := nso.pso.Decode(inputBuf, applyNiovaKV, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return
	}

	log.Trace("Key passed by client: ", applyNiovaKV.InputKey)

	// length of key.
	keyLength := len(applyNiovaKV.InputKey)

	byteToStr := string(applyNiovaKV.InputValue)

	// Length of value.
	valLen := len(byteToStr)

	log.Trace("Write the KeyValue by calling PmdbWriteKV")
	nso.pso.WriteKV(appId, pmdbHandle, applyNiovaKV.InputKey,
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)

}

/*
func (nso *NiovaKVServer) Read(appId unsafe.Pointer, requestBuf unsafe.Pointer,
	requestBufSize int64, replyBuf unsafe.Pointer, replyBufSize int64) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")


	decodeErr := nso.pso.Decode(inputBuf, applyNiovaKV, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return
	}

	log.Trace("Key passed by client: ", applyNiovaKV.InputKey)

	// length of key.
	keyLength := len(applyNiovaKV.InputKey)

	byteToStr := string(applyNiovaKV.InputValue)

	// Length of value.
	valLen := len(byteToStr)

	log.Trace("Write the KeyValue by calling PmdbWriteKV")
	nso.pso.WriteKV(appId, pmdbHandle, applyNiovaKV.InputKey,
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)

}
*/
func (nso *NiovaKVServer) Read(appId unsafe.Pointer, requestBuf unsafe.Pointer,
	requestBufSize int64, replyBuf unsafe.Pointer, replyBufSize int64) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct := &niovakvlib.NiovaKV{}
	decodeErr := nso.pso.Decode(requestBuf, reqStruct, requestBufSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Trace("Key passed by client: ", reqStruct.InputKey)

	keyLen := len(reqStruct.InputKey)
	log.Trace("Key length: ", keyLen)

	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	readResult, readErr := nso.pso.ReadKV(appId, reqStruct.InputKey,
		int64(keyLen), colmfamily)
	var valType []byte
	var replySize int64
	var copyErr error

	if readErr == nil {
		valType = readResult
		inputVal := string(valType)
		log.Trace("Input value after read request:", inputVal)

		resultReq := niovakvlib.NiovaKV{
			InputKey:   reqStruct.InputKey,
			InputValue: valType,
		}

		//Copy the encoded result in replyBuffer
		replySize, copyErr = nso.pso.CopyDataToBuffer(resultReq, replyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		log.Error(readErr)
	}

	log.Trace("Reply size: ", replySize)

	return replySize
}
