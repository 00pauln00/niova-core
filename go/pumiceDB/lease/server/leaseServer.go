package leaseServer

import (
	"bytes"
	leaseLib "common/leaseLib"
	"encoding/gob"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"unsafe"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

var ttlDefault = 60
var LEASE_COLUMN_FAMILY = "NIOVA_LEASE_CF"

type LeaseServerObject struct {
	LeaseColmFam string
	LeaseMap     map[uuid.UUID]*leaseLib.LeaseStruct
	Pso          *PumiceDBServer.PmdbServerObject
}

//Helper functions
func checkMajorCorrectness(currentTerm int64, leaseTerm int64) {
	if currentTerm != leaseTerm {
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

func (lso *LeaseServerObject) InitLeaseObject(pso *PumiceDBServer.PmdbServerObject,
	leaseMap map[uuid.UUID]*leaseLib.LeaseStruct) {
	lso.Pso = pso
	lso.LeaseMap = leaseMap
	lso.LeaseColmFam = LEASE_COLUMN_FAMILY
}

func (lso *LeaseServerObject) GetLeaderTimeStamp(ts *leaseLib.LeaderTS) int {
	var plts PumiceDBServer.PmdbLeaderTS
	rc := PumiceDBServer.PmdbGetLeaderTimeStamp(&plts)
	ts.LeaderTerm = plts.Term
	ts.LeaderTime = plts.Time
	return rc
}

func (lso *LeaseServerObject) Prepare(requestPayload interface{}, reply *interface{}) int {
	var currentTime leaseLib.LeaderTS
	lso.GetLeaderTimeStamp(&currentTime)

	request := requestPayload.(leaseLib.LeaseReq)
	//Check if its a refresh request
	if request.Operation == leaseLib.REFRESH {
		vdev_lease_info, isPresent := lso.LeaseMap[request.Resource]
		if isPresent {
			if isPermitted(vdev_lease_info, request.Client, currentTime, request.Operation) {
				//Refresh the lease
				lso.LeaseMap[request.Resource].TimeStamp = currentTime
				lso.LeaseMap[request.Resource].TTL = ttlDefault
				//Copy the encoded result in replyBuffer
				*reply = *lso.LeaseMap[request.Resource]
				return 0
			}
		}
		return -1
	}

	//Check if get lease
	if request.Operation == leaseLib.GET {
		vdev_lease_info, isPresent := lso.LeaseMap[request.Resource]
		log.Info("Get lease operation")
		if isPresent {
			if !isPermitted(vdev_lease_info, request.Client, currentTime, request.Operation) {
				//Dont provide lease
				return -1
			}
		}
		log.Info("Resource not present in map")
		//Insert or update into MAP
		lso.LeaseMap[request.Resource] = &leaseLib.LeaseStruct{
			Resource:   request.Resource,
			Client:     request.Client,
			LeaseState: leaseLib.INPROGRESS,
		}
	}

	return 1
}

func (lso *LeaseServerObject) ReadLease(requestPayload interface{}, reply *interface{}) int {
	request := requestPayload.(leaseLib.LeaseReq)
	leaseObj, isPresent := lso.LeaseMap[request.Resource]
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
	ttl := leaseObj.TTL - int(leaseObj.TimeStamp.LeaderTime-oldTS.LeaderTime)
	if ttl < 0 {
		leaseObj.TTL = 0
		leaseObj.LeaseState = leaseLib.EXPIRED
	} else {
		leaseObj.TTL = ttl
	}
	*reply = *leaseObj
	return 0
}

func (lso *LeaseServerObject) ApplyLease(requestPayload interface{}, reply *interface{}, userID unsafe.Pointer, pmdbHandler unsafe.Pointer) int {
	request := requestPayload.(leaseLib.LeaseReq)
	leaseObj, isPresent := lso.LeaseMap[request.Resource]
	if !isPresent {
		leaseObj = &leaseLib.LeaseStruct{
			Resource: request.Resource,
			Client:   request.Client,
		}
		lso.LeaseMap[request.Resource] = leaseObj
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
	keyLength := len(request.Resource.String())
	rc := lso.Pso.WriteKV(userID, pmdbHandler, request.Resource.String(), int64(keyLength), byteToStr, int64(valLen), lso.LeaseColmFam)

	if rc < 0 {
		log.Error("Value not written to rocksdb")
		return -1
	}
	if isLeaderFlag == 0 {
		*reply = *leaseObj
		return 0
	}

	return 1
}

func (lso *LeaseServerObject) LeaderInit() {
	for _, leaseObj := range lso.LeaseMap {
		rc := lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
		if rc != 0 {
			log.Error("Unable to get timestamp (InitLeader)")
		}
		leaseObj.TTL = ttlDefault
	}
}

func (lso *LeaseServerObject) PeerBootup(userID unsafe.Pointer) {
	readResult, _, _ := lso.Pso.ReadAllKV(userID, "", 0, 0, lso.LeaseColmFam)

	//Result of the read
	for key, value := range readResult {
		//Decode the request structure sent by client.
		lstruct := &leaseLib.LeaseStruct{}
		dec := gob.NewDecoder(bytes.NewBuffer(value))
		decodeErr := dec.Decode(lstruct)
		if decodeErr != nil {
			log.Error("Failed to decode the read request : ", decodeErr)
			return
		}
		kuuid, _ := uuid.FromString(key)
		lso.LeaseMap[kuuid] = lstruct
		delete(readResult, key)
	}
}
