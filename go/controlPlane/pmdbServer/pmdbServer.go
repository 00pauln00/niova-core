package main

import (
	"bufio"
	"common/requestResponseLib"
	"common/serfAgent"
	compressionLib "common/specificCompressionLib"
	"errors"
	"fmt"
	"flag"
	"io/ioutil"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"os"
	"strings"
	"unsafe"

	//"encoding/json"
	defaultLogger "log"

	log "github.com/sirupsen/logrus"
	//"strconv"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0

// Use the default column family
var colmfamily = "PMDBTS_CF"

type pmdbServerHandler struct {
	raftUUID           string
	peerUUID           string
	logDir             string
	logLevel           string
	gossipClusterFile  string
	gossipClusterNodes []string
	aport              string
	rport              string
	addr               string
	GossipData         map[string]string
	ConfigString       string
	ConfigData         []PeerConfigData
}

type PeerConfigData struct {
	UUID       string
	ClientPort string
	Port       string
	IPAddr     string
}

func main() {
	serverHandler := pmdbServerHandler{}
	nso, pErr := serverHandler.parseArgs()
	if pErr != nil {
		log.Println(pErr)
		return
	}

	switch serverHandler.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}

	fmt.Println(serverHandler.logDir)

	//Create log file
	err := PumiceDBCommon.InitLogger(serverHandler.logDir)
	if err != nil {
		log.Error("Error while initating logger ", err)
		os.Exit(1)
	}

	err = serverHandler.startSerfAgent()
	if err != nil {
		log.Fatal("Error while initializing serf agent ", err)
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
	flag.StringVar(&handler.logLevel, "ll", "Info", "Log level")
	flag.StringVar(&handler.gossipClusterFile, "g", "NULL", "Serf agent port")
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

func extractPMDBServerConfigfromFile(path string) (*PeerConfigData, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	scanner := bufio.NewScanner(f)
	peerData := PeerConfigData{}
	var compressedConfigString, data string

	for scanner.Scan() {
		text := scanner.Text()
		lastIndex := len(strings.Split(text, " ")) - 1
		key := strings.Split(text, " ")[0]
		value := strings.Split(text, " ")[lastIndex]
		switch key {
		case "CLIENT_PORT":
			peerData.ClientPort = value
			data, err = compressionLib.CompressStringNumber(value, 2)
			compressedConfigString += data
		case "IPADDR":
			peerData.IPAddr = value
			data, err = compressionLib.CompressIPV4(value)
			compressedConfigString += data
		case "PORT":
			peerData.Port = value
			data, err = compressionLib.CompressStringNumber(value, 2)
			compressedConfigString += data
		}
		if err != nil {
			return nil, "", err
		}
	}
	f.Close()

	return &peerData, compressedConfigString, err
}

func (handler *pmdbServerHandler) readPMDBServerConfig() error {
	folder := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	files, err := ioutil.ReadDir(folder + "/")
	if err != nil {
		return err
	}
	handler.GossipData = make(map[string]string)

	for _, file := range files {
		if strings.Contains(file.Name(), ".peer") {
			//Extract required config from the file
			path := folder + "/" + file.Name()
			peerData, compressedConfigString, err := extractPMDBServerConfigfromFile(path)
			if err != nil {
				return err
			}

			uuid := file.Name()[:len(file.Name())-5]
			cuuid, err := compressionLib.CompressUUID(uuid)
			if err != nil {
				return err
			}

			handler.GossipData[cuuid] = compressedConfigString
			peerData.UUID = uuid
			handler.ConfigData = append(handler.ConfigData, *peerData)
		}
	}

	return nil
}

func (handler *pmdbServerHandler) readGossipClusterFile() error {
	f, err := os.Open(handler.gossipClusterFile)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		text := scanner.Text()
		splitData := strings.Split(text, " ")
		addr := splitData[1]
		aport := splitData[2]
		rport := splitData[3]
		uuid := splitData[0]
		if uuid == handler.peerUUID {
			handler.aport = aport
			handler.rport = rport
			handler.addr = addr
		} else {
			handler.gossipClusterNodes = append(handler.gossipClusterNodes, addr+":"+aport)
		}
	}

	if handler.addr == "" {
		log.Error("Peer UUID not matching with gossipNodes config file")
		return errors.New("UUID not matching")
	}

	log.Info("Cluster nodes : ", handler.gossipClusterNodes)
	log.Info("Node serf info : ", handler.addr, handler.aport, handler.rport)
	return nil
}

func (handler *pmdbServerHandler) startSerfAgent() error {
	err := handler.readGossipClusterFile()
	if err != nil {
		return err
	}
	serfLog := "00"
	switch serfLog {
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
	serfAgentHandler := serfAgent.SerfAgentHandler{
		Name:     handler.peerUUID,
		BindAddr: handler.addr,
	}
	serfAgentHandler.BindPort = handler.aport
	serfAgentHandler.AgentLogger = defaultLogger.Default()
	serfAgentHandler.RpcAddr = handler.addr
	serfAgentHandler.RpcPort = handler.rport
	//Start serf agent
	_, err = serfAgentHandler.SerfAgentStartup(handler.gossipClusterNodes, true)
	if err != nil {
		log.Error("Error while starting serf agent ", err)
	}
	handler.readPMDBServerConfig()
	handler.GossipData["Type"] = "PMDB_SERVER"
	handler.GossipData["Rport"] = handler.rport
	handler.GossipData["RU"] = handler.raftUUID
	log.Info(handler.GossipData)
	serfAgentHandler.SetNodeTags(handler.GossipData)
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

	log.Info("NiovaCtlPlane server: Apply request received")

	// Decode the input buffer into structure format
	applyNiovaKV := &requestResponseLib.KVRequest{}

	decodeErr := nso.pso.Decode(inputBuf, applyNiovaKV, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return
	}

	log.Info("Key passed by client: ", applyNiovaKV.Key)

	// length of key.
	keyLength := len(applyNiovaKV.Key)

	byteToStr := string(applyNiovaKV.Value)

	// Length of value.
	valLen := len(byteToStr)

	log.Info("Write the KeyValue by calling PmdbWriteKV")
	nso.pso.WriteKV(appId, pmdbHandle, applyNiovaKV.Key,
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
	reqStruct := &requestResponseLib.KVRequest{}
	decodeErr := nso.pso.Decode(requestBuf, reqStruct, requestBufSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Info("Key passed by client: ", reqStruct.Key)
	log.Info("Last Key Read by client: ", reqStruct.LastKeyRead)
	keyLen := len(reqStruct.Key)
	log.Trace("Key length: ", keyLen)

	var readResult = make(map[string]string)
	var lastKey string
	var readErr error
	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	log.Info("reqStruct.RangeQuery = ", reqStruct.RangeQuery)
	if reqStruct.RangeQuery {
		log.Info("Calling range query")
		lastKeyLen := len(reqStruct.LastKeyRead)
		readResult, lastKey, readErr = nso.pso.RangeReadKV(appId, reqStruct.Key, int64(keyLen), reqStruct.LastKeyRead, int64(lastKeyLen), replyBufSize, colmfamily)
	}else{
		log.Info("Calling point query")
		readResult, readErr = nso.pso.ReadKV(appId, reqStruct.Key, int64(keyLen), colmfamily)
	}
	var replySize int64
	var copyErr error


	log.Info(readResult)
	if readErr == nil {

		resultReq := requestResponseLib.KVResponse{
			Status: 0,
			Value: readResult,
			LastKeyRead: lastKey,
			//XXX return lastKey as well
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
