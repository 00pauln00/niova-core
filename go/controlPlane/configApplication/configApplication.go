package main

import (
 "os"
 "fmt"
 "common/requestResponseLib"
 "encoding/json"
 "encoding/gob"
 "errors"
 "common/serfClient"
 compressionLib "common/specificCompressionLib"
 "flag"
 uuid "github.com/satori/go.uuid"
 pmdbClient "niova/go-pumicedb-lib/client"
 PumiceDBCommon "niova/go-pumicedb-lib/common"
 "bytes"
 "strconv"
 "hash/crc32"
 "sort"
 "time"
 "encoding/binary"
 log "github.com/sirupsen/logrus"
)


type configApplication struct {
	pmdbClientObj *pmdbClient.PmdbClientObj
	clientUUID string
	raftUUID string
	portRange string
	gossipNodesFile string
	read bool
	PMDBServerConfigArray []PumiceDBCommon.PeerConfigData
}

func (handler *configApplication) getCmdLineArgs() {
	flag.StringVar(&handler.raftUUID, "r", "NULL", "Raft UUID")
	flag.StringVar(&handler.portRange, "p", "NULL", "Port range [0-9]")
	flag.StringVar(&handler.gossipNodesFile, "g", "./gossipNodes", "Gossip Nodes file to connect with gossip mesh")
	flag.BoolVar(&handler.read, "o", false, "Read port range information")
}

func getAnyEntryFromStringMap(mapSample map[string]map[string]string) map[string]string {
        for _, v := range mapSample {
                return v
        }
        return nil
}

func validateCheckSum(data map[string]string, checksum string) error {
        keys := make([]string, 0, len(data))
        var allDataArray []string

        //Append map keys to key array
        for k := range data {
                keys = append(keys, k)
        }

        //Sort the key array to ensure uniformity
        sort.Strings(keys)
        //Iterate over the sorted keys and append the value to array
        for _, k := range keys {
                allDataArray = append(allDataArray, k+data[k])
        }

        //Convert value array to byte slice
        byteArray, err := json.Marshal(allDataArray)
        if err != nil {
                return err
        }

        //Calculate checksum for the byte slice
        calculatedChecksum := crc32.ChecksumIEEE(byteArray)

        //Convert the checksum to uint32 and compare with identified checksum
        convertedCheckSum := binary.LittleEndian.Uint32([]byte(checksum))
        if calculatedChecksum != convertedCheckSum {
                return errors.New("Checksum mismatch")
        }
        return nil
}

func (handler *configApplication) dumpConfigToFile(outfilepath string) error {
        //Generate .raft
	raft_file, err := os.Create(outfilepath + (handler.raftUUID) + ".raft")
        if err != nil {
                return err
        }

        _, errFile := raft_file.WriteString("RAFT " + (handler.raftUUID) + "\n")
        if errFile != nil {
                return err
        }

        for _, peer := range handler.PMDBServerConfigArray {
                raft_file.WriteString("PEER " + (uuid.UUID(peer.UUID).String()) + "\n")
        }

        raft_file.Sync()
        raft_file.Close()

        //Generate .peer
        for _, peer := range handler.PMDBServerConfigArray {
                peer_file, err := os.Create(outfilepath + (uuid.UUID(peer.UUID).String()) + ".peer")
                if err != nil {
                        log.Error(err)
                }

                _, errFile := peer_file.WriteString(
                        "RAFT         " + (handler.raftUUID) +
                                "\nIPADDR       " + peer.IPAddr.String() +
                                "\nPORT         " + strconv.Itoa(int(peer.Port)) +
                                "\nCLIENT_PORT  " + strconv.Itoa(int(peer.ClientPort)) +
                                "\nSTORE        ./*.raftdb\n")

                if errFile != nil {
                        return errFile
                }
                peer_file.Sync()
                peer_file.Close()
        }
        return nil
}



func (handler *configApplication) GetPMDBServerConfig() error {
        //Init serf client
	serfClientObj := serfClient.SerfClientHandler{}
        serfClientObj.InitData(handler.gossipNodesFile)

        //Iterate till getting PMDB config data from serf gossip
        var allPmdbServerGossip map[string]map[string]string
        for len(allPmdbServerGossip) == 0 {
                allPmdbServerGossip = serfClientObj.GetTags("Type", "PMDB_SERVER")
                time.Sleep(2 * time.Second)
        }
        log.Info("PMDB config from gossip : ", allPmdbServerGossip)

        var err error
        pmdbServerGossip := getAnyEntryFromStringMap(allPmdbServerGossip)

	//Get Raft UUID from the map
        handler.raftUUID = pmdbServerGossip["RU"]
        if err != nil {
                log.Error("Error :", err)
                return err
        }

        //Validate checksum; Get checksum entry from Map and delete that entry
        recvCheckSum := pmdbServerGossip["CS"]
        delete(pmdbServerGossip, "CS")
        err = validateCheckSum(pmdbServerGossip, recvCheckSum)
        if err != nil {
                return err
        }

        //Get PMDB config from the map
        for key, value := range pmdbServerGossip {
		_, err := compressionLib.DecompressUUID(key)
                if err == nil {
                        peerConfig := PumiceDBCommon.PeerConfigData{}
                        compressionLib.DecompressStructure(&peerConfig, key+value)
                        log.Info("Peer config : ", peerConfig)
                        handler.PMDBServerConfigArray = append(handler.PMDBServerConfigArray, peerConfig)
                }
	}

	log.Info("Decompressed PMDB server config array : ", handler.PMDBServerConfigArray)
        path := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
        //Create PMDB server config dir
        os.Mkdir(path, os.ModePerm)
        return handler.dumpConfigToFile(path + "/")
}



func (handler *configApplication) startPMDBClient() error {
        var err error
	fmt.Println("Raft uuid : ", handler.raftUUID)
        //handler.raftUUID = "c077b532-44f2-11ed-822f-72e5126963a0"
	//Get client object
        handler.pmdbClientObj = pmdbClient.PmdbClientNew(handler.raftUUID, handler.clientUUID)
        if handler.pmdbClientObj == nil {
                return errors.New("PMDB client object is empty")
        }

        //Start pumicedb client
        err = handler.pmdbClientObj.Start()
        if err != nil {
                return err
        }
	
	leaderUuid, err := handler.pmdbClientObj.PmdbGetLeader()
	for (err != nil){
	    leaderUuid, err = handler.pmdbClientObj.PmdbGetLeader()
	}
	fmt.Println("Leader uuid : ",leaderUuid.String());
	// Defer the Stop
        //defer handler.pmdbClientObj.Stop()

        //Store rncui in clientObj
        handler.pmdbClientObj.AppUUID = uuid.NewV4().String()
        return nil

}

func (handler *configApplication) Write(key string, data []byte) error{
	var request requestResponseLib.KVRequest
	request.Operation = "write"
	request.Key = key
	request.Value = data
	request.Rncui = uuid.NewV4().String()+":0:0:0:0"
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	enc.Encode(request)
	err := handler.pmdbClientObj.WriteEncoded(requestBytes.Bytes(), request.Rncui)
	return err
}



func (handler *configApplication) Read(key string, response *[]byte) error {
        var request requestResponseLib.KVRequest
        request.Operation = "read"
        request.Key = key
        var requestBytes bytes.Buffer
        enc := gob.NewEncoder(&requestBytes)
        enc.Encode(request)
	return handler.pmdbClientObj.ReadEncoded(requestBytes.Bytes(), "", response)
}


func main() {
	appHandler := configApplication{
		clientUUID:uuid.NewV4().String(),
	}
	
	appHandler.getCmdLineArgs()
	flag.Parse()
	appHandler.GetPMDBServerConfig()
	err := appHandler.startPMDBClient()
	fmt.Println("PMDB client error : ", err);
	
	if !appHandler.read {
		err = appHandler.Write(appHandler.raftUUID+"_Port_Range", []byte(appHandler.portRange));
	} else {
		var value []byte
		err = appHandler.Read(appHandler.raftUUID+"_Port_Range", &value)
		var response requestResponseLib.KVResponse
		dec := gob.NewDecoder(bytes.NewBuffer(value))
		dec.Decode(&response)
		fmt.Println("Value : ",string(response.Value))
	}
	fmt.Println("Error in operation : ", err);
}
