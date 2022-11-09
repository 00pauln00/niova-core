package main

import (
	"bufio"
	"common/lookout"
	"common/requestResponseLib"
	"common/serfAgent"
	compressionLib "common/specificCompressionLib"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"hash/crc32"
	"io/ioutil"
	defaultLogger "log"
	"net"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"os"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0

var encodingOverhead = 256

// Use the default column family
var colmfamily = "PMDBTS_CF"

type pmdbServerHandler struct {
	raftUUID           uuid.UUID
	peerUUID           uuid.UUID
	logDir             string
	logLevel           string
	gossipClusterFile  string
	servicePortRangeS  uint16
	servicePortRangeE  uint16
	hport              uint16
	prometheus         bool
	nodeAddr           net.IP
	GossipData         map[string]string
	ConfigString       string
	ConfigData         []PumiceDBCommon.PeerConfigData
	lookoutInstance    lookout.EPContainer
	serfAgentHandler   serfAgent.SerfAgentHandler
	portRange	   []int16
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

	log.Info("Log Dir - ", serverHandler.logDir)

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

	//Start lookout monitoring
	CTL_SVC_DIR_PATH := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	fmt.Println("Path CTL :  ", CTL_SVC_DIR_PATH[:len(CTL_SVC_DIR_PATH)-7]+"ctl-interface/")
	ctl_path := CTL_SVC_DIR_PATH[:len(CTL_SVC_DIR_PATH)-7] + "ctl-interface/"
	serverHandler.lookoutInstance = lookout.EPContainer{
		MonitorUUID:      nso.peerUuid.String(),
		AppType:          "PMDB",
		//HttpPort:         int(serverHandler.hport),
		PortRange:	  serverHandler.portRange[20:40],
		CTLPath:          ctl_path,
		SerfMembershipCB: serverHandler.SerfMembership,
		EnableHttp:       serverHandler.prometheus,
	}
	go serverHandler.lookoutInstance.Start()

	nso.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: colmfamily,
		RaftUuid:       nso.raftUuid.String(),
		PeerUuid:       nso.peerUuid.String(),
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

func makeRange(min, max int) []int16 {
a := make([]int16, max-min+1)
for i := range a {
    a[i] = int16(min + i)
}
return a
}

func (handler *pmdbServerHandler) parseArgs() (*NiovaKVServer, error) {
	var tempRaftUUID, tempPeerUUID string
	var err error

	flag.StringVar(&tempRaftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&tempPeerUUID, "u", "NULL", "peer uuid")

	/* If log path is not provided, it will use Default log path.
	   default log path: /tmp/<peer-uuid>.log
	*/
	defaultLog := "/" + "tmp" + "/" + handler.peerUUID.String() + ".log"
	flag.StringVar(&handler.logDir, "l", defaultLog, "log dir")
	flag.StringVar(&handler.logLevel, "ll", "Info", "Log level")
	flag.StringVar(&handler.gossipClusterFile, "g", "NULL", "Serf agent port")
	flag.BoolVar(&handler.prometheus, "p", false, "Enable prometheus")
	flag.Parse()

	handler.portRange = makeRange(6000,7000)
	handler.raftUUID, _ = uuid.FromString(tempRaftUUID)
	handler.peerUUID, _ = uuid.FromString(tempPeerUUID)
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

func (handler *pmdbServerHandler) SerfMembership() map[string]bool {
	membership := handler.serfAgentHandler.GetMembersState()
	return membership
}

func extractPMDBServerConfigfromFile(path string) (*PumiceDBCommon.PeerConfigData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	peerData := PumiceDBCommon.PeerConfigData{}

	for scanner.Scan() {
		text := scanner.Text()
		lastIndex := len(strings.Split(text, " ")) - 1
		key := strings.Split(text, " ")[0]
		value := strings.Split(text, " ")[lastIndex]
		switch key {
		case "CLIENT_PORT":
			buffer, err := strconv.ParseUint(value, 10, 16)
			peerData.ClientPort = uint16(buffer)
			if err != nil {
				return nil, errors.New("Client Port is out of range")
			}

		case "IPADDR":
			peerData.IPAddr = net.ParseIP(value)

		case "PORT":
			buffer, err := strconv.ParseUint(value, 10, 16)
			peerData.Port = uint16(buffer)
			if err != nil {
				return nil, errors.New("Port is out of range")
			}
		}
	}
	f.Close()

	return &peerData, err
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
			peerData, err := extractPMDBServerConfigfromFile(path)
			if err != nil {
				return err
			}

			uuid := file.Name()[:len(file.Name())-5]
			cuuid, err := compressionLib.CompressUUID(uuid)
			if err != nil {
				return err
			}

			tempGossipData, _ := compressionLib.CompressStructure(*peerData)
			handler.GossipData[cuuid] = tempGossipData[16:]
			//Since the uuid filled after compression, the cuuid wont be included in compressed string
			copy(peerData.UUID[:], []byte(cuuid))
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
	/*
	Following is the format of gossipNodes file
	PMDB server addrs with space separated
	Start_port End_port
	*/
	scanner.Scan()
	//Read Ports
	scanner.Scan()
	Ports := strings.Split(scanner.Text(), " ")
	handler.servicePortRangeS = uint16(Ports[0])
	handler.servicePortRangeE = uint16(Ports[1])
	return nil
}

func generateCheckSum(data map[string]string) (string, error) {
	keys := make([]string, 0, len(data))
	var allDataArray []string
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		allDataArray = append(allDataArray, k+data[k])
	}

	byteArray, err := json.Marshal(allDataArray)
	checksum := crc32.ChecksumIEEE(byteArray)
	checkSumByteArray := make([]byte, 4)
	binary.LittleEndian.PutUint32(checkSumByteArray, uint32(checksum))
	return string(checkSumByteArray), err
}

func (handler *pmdbServerHandler) getAddrList() []string {
	var addrs []string
	for i := 0;i <= 20;i++ {
		fmt.Println(handler.nodeAddr.String() + ":" + strconv.Itoa(int(handler.portRange[i])))
		addrs = append(addrs, handler.nodeAddr.String() + ":" + strconv.Itoa(int(handler.portRange[i])))
	}
	return addrs
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
		Name:     		handler.peerUUID.String(),
		BindAddr: 		handler.nodeAddr,
		AgentLogger: 		defaultLogger.Default(),
		RpcAddr: 		handler.nodeAddr,
		ServicePortRangeS: 	handler.servicePortRangeS,
		ServicePortRangeE: 	handler.servicePortRangeE,
	}
<<<<<<< Updated upstream

	joinAddrs := handler.getAddrList()

	//Start serf agent
	_, err = serfAgentHandler.SerfAgentStartup(joinAddrs, true)
=======
	serfAgentHandler.AgentLogger = defaultLogger.Default()
	serfAgentHandler.RpcAddr = handler.nodeAddr
	serfAgentHandler.ServicePortRangeS = handler.servicePortRangeS
	serfAgentHandler.ServicePortRangeE = handler.servicePortRangeE
	err = serfAgentHandler.ReadGossipNodesFile(handler.gossipClusterFile)
	if err != nil {
		log.Error("Error while reading gossipNodes file in serf agent ", err)
	}
	//Start serf agent
	_, err = serfAgentHandler.SerfAgentStartup(true)
>>>>>>> Stashed changes
	if err != nil {
		log.Error("Error while starting serf agent ", err)
	}
	handler.readPMDBServerConfig()
	handler.GossipData["Type"] = "PMDB_SERVER"
	//handler.GossipData["Rport"] = strconv.Itoa(int(handler.serfRPCPort))
	handler.GossipData["RU"] = handler.raftUUID.String()
	handler.GossipData["CS"], err = generateCheckSum(handler.GossipData)
	if err != nil {
		return err
	}
	log.Info(handler.GossipData)
	serfAgentHandler.SetNodeTags(handler.GossipData)
	handler.serfAgentHandler = serfAgentHandler
	return err
}

type NiovaKVServer struct {
	raftUuid       uuid.UUID
	peerUuid       uuid.UUID
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

func (nso *NiovaKVServer) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHandle unsafe.Pointer) int {

	log.Trace("NiovaCtlPlane server: Apply request received")

	// Decode the input buffer into structure format
	applyNiovaKV := &requestResponseLib.KVRequest{}
	decodeErr := nso.pso.Decode(inputBuf, applyNiovaKV, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Trace("Key passed by client: ", applyNiovaKV.Key)

	// length of key.
	keyLength := len(applyNiovaKV.Key)

	byteToStr := string(applyNiovaKV.Value)

	// Length of value.
	valLen := len(byteToStr)

	log.Info("Write the KeyValue by calling PmdbWriteKV")
	rc := nso.pso.WriteKV(appId, pmdbHandle, applyNiovaKV.Key,
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)
	return rc
}

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

	log.Trace("Key passed by client: ", reqStruct.Key)
	keyLen := len(reqStruct.Key)
	log.Trace("Key length: ", keyLen)

	var readErr error
	var resultResponse requestResponseLib.KVResponse
	//var resultReq requestResponseLib.KVResponse
	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	if reqStruct.Operation == "read" {

		log.Trace("read - ", reqStruct.SeqNum)
		readResult, err := nso.pso.ReadKV(appId, reqStruct.Key,
			int64(keyLen), colmfamily)
		singleReadMap := make(map[string][]byte)
		singleReadMap[reqStruct.Key] = readResult
		resultResponse = requestResponseLib.KVResponse{
			Key:       reqStruct.Key,
			ResultMap: singleReadMap,
		}
		readErr = err

	} else if reqStruct.Operation == "rangeRead" {
		reqStruct.Prefix = reqStruct.Prefix
		log.Trace("sequence number - ", reqStruct.SeqNum)
		readResult, lastKey, seqNum, snapMiss, err := nso.pso.RangeReadKV(appId, reqStruct.Key,
			int64(keyLen), reqStruct.Prefix, (replyBufSize - int64(encodingOverhead)), reqStruct.Consistent, reqStruct.SeqNum, colmfamily)
		var cRead bool
		if lastKey != "" {
			cRead = true
		} else {
			cRead = false
		}
		resultResponse = requestResponseLib.KVResponse{
			Prefix:       reqStruct.Key,
			ResultMap:    readResult,
			ContinueRead: cRead,
			Key:          lastKey,
			SeqNum:       seqNum,
			SnapMiss:     snapMiss,
		}
		readErr = err
	}

	log.Trace("Response trace : ", resultResponse)
	var replySize int64
	var copyErr error
	if readErr == nil {
		//Copy the encoded result in replyBuffer
		replySize, copyErr = nso.pso.CopyDataToBuffer(resultResponse, replyBuf)
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
