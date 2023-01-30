package leaseServer

import (
	"encoding/gob"
	"bytes"
	"unsafe"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	uuid "github.com/google/uuid"
        log "github.com/sirupsen/logrus"
	leaseLib "common/leaseLib"
)

var ttlDefault = 60
type LeaseServerObject struct {
	UserID	    unsafe.Pointer
	PmdbHandler unsafe.Pointer
	LeaseColmFam string
	LeaseMap map[uuid.UUID]*leaseLib.LeaseStruct
	Pso      *PumiceDBServer.PmdbServerObject
}



//Helper functions
func checkMajorCorrectness(currentTerm int64, leaseTerm int64) {
        if(currentTerm != leaseTerm) {
                log.Fatal("Major(Term) not matching")
        }
}

func isPermitted(entry *leaseLib.LeaseStruct, clientUUID uuid.UUID, currentTime leaseLib.LeaderTS, operation int) bool {
        if entry.LeaseState == leaseLib.INPROGRESS {
                return false
        }
        leaseExpiryTS := entry.TimeStamp.LeaderTime + int64(entry.TTL)

        //Check majot correctness
	checkMajorCorrectness(currentTime.LeaderTerm, entry.TimeStamp.LeaderTerm)
        //Check if existing lease is valid; by comparing only the minor
        stillValid := leaseExpiryTS >= currentTime.LeaderTime
	//If valid, Check if client uuid is same and operation is refresh
        if stillValid {
                if (operation == leaseLib.REFRESH) && (clientUUID == entry.Client) {
                        return true
                } else {
                        return false
                }
        }
        return true
}



func (lso *LeaseServerObject) GetLeaderTimeStamp(ts *leaseLib.LeaderTS) int {
        var plts PumiceDBServer.PmdbLeaderTS
        rc := PumiceDBServer.PmdbGetLeaderTimeStamp(&plts)
        ts.LeaderTerm = plts.LeaderTerm
        ts.LeaderTime = plts.LeaderTime
        return rc
}

func (lso *LeaseServerObject) Prepare(opcode int, resourceUUID uuid.UUID, clientUUID uuid.UUID, reply *interface{}) int {
	var currentTime leaseLib.LeaderTS
        lso.GetLeaderTimeStamp(&currentTime)

        //Check if its a refresh request
        if opcode == leaseLib.REFRESH {
		vdev_lease_info, isPresent := lso.LeaseMap[resourceUUID]
                if isPresent {
                        if isPermitted(vdev_lease_info, clientUUID, currentTime, opcode) {
                                //Refresh the lease
                                lso.LeaseMap[resourceUUID].TimeStamp = currentTime
                                lso.LeaseMap[resourceUUID].TTL = ttlDefault
                                //Copy the encoded result in replyBuffer
                               	*reply = *lso.LeaseMap[resourceUUID]
                                return 0
                        }
                }
                return -1
        }

        //Check if get lease
        if opcode == leaseLib.GET {
                vdev_lease_info, isPresent := lso.LeaseMap[resourceUUID]	
		log.Info("Get lease operation")
                if isPresent {
                        if !isPermitted(vdev_lease_info, clientUUID, currentTime, opcode) {
                                //Dont provide lease
                                return -1
                        }
                }
                log.Info("Resource not present in map")
                //Insert or update into MAP
                lso.LeaseMap[resourceUUID] = &leaseLib.LeaseStruct{
                        Resource:   resourceUUID,
                        Client:     clientUUID,
                        LeaseState: leaseLib.INPROGRESS,
                }
        }

        return 1	
}



func (lso *LeaseServerObject) ReadLease(resourceUUID uuid.UUID, reply *interface{}) int {
	leaseObj, isPresent := lso.LeaseMap[resourceUUID]
	if !isPresent {
		return -1
	}

	if leaseObj.LeaseState == leaseLib.INPROGRESS {
                        return -1
        }

        oldTS := leaseObj.TimeStamp
        lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
        checkMajorCorrectness(leaseObj.TimeStamp.LeaderTerm, oldTS.LeaderTerm)
	
	//TODO: Possible wrap around
        ttl := ttlDefault - int(leaseObj.TimeStamp.LeaderTime - oldTS.LeaderTime)
        if ttl < 0 {
        	leaseObj.TTL = 0
                leaseObj.LeaseState = leaseLib.EXPIRED
        } else {
               	leaseObj.TTL = ttl
        }
	*reply = *leaseObj
	return 0
}

func (lso *LeaseServerObject) ApplyLease(resourceUUID uuid.UUID, clientUUID uuid.UUID, reply *interface{}) int {
	leaseObj, isPresent := lso.LeaseMap[resourceUUID]
        if !isPresent {
                leaseObj = &leaseLib.LeaseStruct{
                        Resource: resourceUUID,
                        Client:   clientUUID,
                }
                lso.LeaseMap[resourceUUID] = leaseObj
        }
        leaseObj.LeaseState = leaseLib.GRANTED
        isLeaderFlag := lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
        leaseObj.TTL = ttlDefault
	
	var valueBytes bytes.Buffer
	enc := gob.NewEncoder(&valueBytes)
        err := enc.Encode(&leaseObj)
        if err != nil {
                log.Error(err)
        }

        byteToStr := string(valueBytes.Bytes())
        log.Trace("Value passed by client: ", byteToStr)

        // Length of value.
        valLen := len(byteToStr)
	keyLength := len(resourceUUID.String())
        rc := lso.Pso.WriteKV(lso.UserID, lso.PmdbHandler, resourceUUID.String(), int64(keyLength), byteToStr, int64(valLen), lso.LeaseColmFam)

        if rc < 0 {
                log.Error("Value not written to rocksdb")
                return -1
        }
	if (isLeaderFlag == 0){
		return 0
	}

	*reply = *leaseObj
	return 1
}

func (lso *LeaseServerObject) PeerBootup() {
	readResult, _, _, _, err := lso.Pso.RangeReadKV(lso.UserID, "",
        0, "", 0, true, 0, lso.LeaseColmFam)
	
        log.Info("Read result : ",readResult)
        if err != nil {
        	log.Error("Failed range query : ", err)
                return
        }

        //Result of the read
        for key,value := range readResult {
        	//Decode the request structure sent by client.
                lstruct := &leaseLib.LeaseStruct{}
                dec := gob.NewDecoder(bytes.NewBuffer(value))
                decodeErr := dec.Decode(lstruct)

                if decodeErr != nil {
                	log.Error("Failed to decode the read request : ", decodeErr)
                        return
                }
                kuuid, _ := uuid.Parse(key)
		lso.LeaseMap[kuuid] = lstruct
         }
}
