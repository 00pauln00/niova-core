package leaseServer

import (
	"bytes"
	leaseLib "common/leaseLib"
	list "container/list"
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
	LeaseMap     map[uuid.UUID]*leaseLib.LeaseMeta
	Pso          *PumiceDBServer.PmdbServerObject
	listObj      *list.List
}

//Helper functions
func checkMajorCorrectness(currentTerm int64, leaseTerm int64) {
	if currentTerm != leaseTerm {
		log.Fatal("Major(Term) not matching")
	}
}

func copyToResponse(lease *leaseLib.LeaseMeta, response *leaseLib.LeaseRes) {
	response.Resource = lease.Resource
	response.Client = lease.Client
	response.LeaseState = lease.LeaseState
	response.TTL = lease.TTL
	response.TimeStamp = lease.TimeStamp
}

func isPermitted(entry *leaseLib.LeaseMeta, clientUUID uuid.UUID, currentTime leaseLib.LeaderTS, operation int) bool {
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

func Decode(payload []byte) (leaseLib.LeaseReq, error) {
	request := &leaseLib.LeaseReq{}
	dec := gob.NewDecoder(bytes.NewBuffer(payload))
	err := dec.Decode(request)
	return *request, err
}

func (lso *LeaseServerObject) InitLeaseObject(pso *PumiceDBServer.PmdbServerObject) {
	lso.Pso = pso
	lso.LeaseMap = make(map[uuid.UUID]*leaseLib.LeaseMeta)
	lso.LeaseColmFam = LEASE_COLUMN_FAMILY
	//Register Lease callbacks
	lso.Pso.LeaseAPI = lso
	lso.listObj = list.New()
}

func (lso *LeaseServerObject) GetLeaderTimeStamp(ts *leaseLib.LeaderTS) int {
	var plts PumiceDBServer.PmdbLeaderTS
	rc := PumiceDBServer.PmdbGetLeaderTimeStamp(&plts)
	ts.LeaderTerm = plts.Term
	ts.LeaderTime = plts.Time
	return rc
}

func (lso *LeaseServerObject) prepare(request leaseLib.LeaseReq, reply *leaseLib.LeaseRes) int {
	var currentTime leaseLib.LeaderTS
	lso.GetLeaderTimeStamp(&currentTime)

	//Check if its a refresh request
	if request.Operation == leaseLib.REFRESH {
		vdev_lease_info, isPresent := lso.LeaseMap[request.Resource]
		if isPresent {
			if isPermitted(vdev_lease_info, request.Client, currentTime, request.Operation) {
				//Refresh the lease
				lso.LeaseMap[request.Resource].TimeStamp = currentTime
				lso.LeaseMap[request.Resource].TTL = ttlDefault
				//Copy the encoded result in replyBuffer
				//TODO: Pointer leak from list element
				copyToResponse(lso.LeaseMap[request.Resource], reply)
				//Update lease list
				lso.listObj.MoveToBack(lso.LeaseMap[request.Resource].ListElement)
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
		lso.LeaseMap[request.Resource] = &leaseLib.LeaseMeta{
			Resource:   request.Resource,
			Client:     request.Client,
			LeaseState: leaseLib.INPROGRESS,
		}
	}

	return 1
}

func (lso *LeaseServerObject) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {

	var copyErr error
	var replySize int64

	//Decode request
	request, err := Decode(wrPrepArgs.Payload)
	if err != nil {
		log.Error(err)
		return -1
	}

	var returnObj leaseLib.LeaseRes
	rc := lso.prepare(request, &returnObj)

	if rc <= 0 {
		//Dont continue write
		_, copyErr = lso.Pso.CopyDataToBuffer(byte(0), wrPrepArgs.ContinueWr)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
		if rc == 0 {
			replySize, copyErr = lso.Pso.CopyDataToBuffer(returnObj, wrPrepArgs.ReplyBuf)
			if copyErr != nil {
				log.Error("Failed to Copy result in the buffer: %s", copyErr)
				return -1
			}
			return replySize
		}
		return -1
	} else {
		//Continue write
		_, copyErr = lso.Pso.CopyDataToBuffer(byte(1), wrPrepArgs.ContinueWr)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
		return 0
	}

}

func (lso *LeaseServerObject) readLease(request leaseLib.LeaseReq, reply *leaseLib.LeaseRes) int {
	if request.Operation == leaseLib.LOOKUP {
		leaseObj, isPresent := lso.LeaseMap[request.Resource]
		if !isPresent {
			return -1
		}
		leaseObj.TTL = -1
		leaseObj.LeaseState = leaseLib.NULL
		lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
		copyToResponse(leaseObj, reply)

	} else if request.Operation == leaseLib.LOOKUP_VALIDATE {
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
		//*reply = *leaseObj
		reply.Client = leaseObj.Client
		reply.Resource = leaseObj.Resource
		reply.Status = leaseObj.Status
		reply.LeaseState = leaseObj.LeaseState
		reply.TTL = leaseObj.TTL
		reply.TimeStamp = leaseObj.TimeStamp
	}
	return 0
}

func (lso *LeaseServerObject) Read(readArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct, err := Decode(readArgs.Payload)
	if err != nil {
		log.Error(err)
		return -1
	}

	log.Trace("Key passed by client: ", reqStruct.Resource)
	var returnObj leaseLib.LeaseRes
	var replySize int64
	var copyErr error

	rc := lso.readLease(reqStruct, &returnObj)

	if rc == 0 {
		replySize, copyErr = lso.Pso.CopyDataToBuffer(returnObj, readArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}

		return replySize
	}

	return int64(rc)

}

func (lso *LeaseServerObject) applyLease(request leaseLib.LeaseReq, reply *leaseLib.LeaseRes, userID unsafe.Pointer, pmdbHandler unsafe.Pointer) int {
	leaseObj, isPresent := lso.LeaseMap[request.Resource]
	if !isPresent {
		leaseObj = &leaseLib.LeaseMeta{
			Resource: request.Resource,
			Client:   request.Client,
		}
		lso.LeaseMap[request.Resource] = leaseObj
	}
	leaseObj.LeaseState = leaseLib.GRANTED
	isLeaderFlag := lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
	leaseObj.TTL = ttlDefault

	//Insert into list
	leaseObj.ListElement = &list.Element{}
	leaseObj.ListElement.Value = leaseObj
	lso.listObj.PushBack(leaseObj)

	var valueBytes bytes.Buffer
	//gob.Register(leaseLib.LeaseStruct{})
	enc := gob.NewEncoder(&valueBytes)
	err := enc.Encode(leaseObj)
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
		leaseObj.Status = "Key not found"
		log.Error("Value not written to rocksdb")
		return -1
	} else {
		leaseObj.Status = "Success"
	}

	if isLeaderFlag == 0 {
		//*reply = *leaseObj
		reply.Client = leaseObj.Client
		reply.Resource = leaseObj.Resource
		reply.Status = leaseObj.Status
		reply.LeaseState = leaseObj.LeaseState
		reply.TTL = leaseObj.TTL
		reply.TimeStamp = leaseObj.TimeStamp
		return 0
	}

	return 1
}

func (lso *LeaseServerObject) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {
	//var valueBytes bytes.Buffer
	var copyErr error
	var replySizeRc int64

	// Decode the input buffer into structure format
	applyLeaseReq, err := Decode(applyArgs.Payload)
	if err != nil {
		log.Error(err)
		return -1
	}

	log.Info("(Apply) Lease request by client : ", applyLeaseReq.Client.String(), " for resource : ", applyLeaseReq.Resource.String())

	// length of key.
	//keyLength := len(applyLeaseReq.Client.String())

	var returnObj leaseLib.LeaseRes
	rc := lso.applyLease(applyLeaseReq, &returnObj, applyArgs.UserID, applyArgs.PmdbHandler)
	//Copy the encoded result in replyBuffer
	replySizeRc = 0
	if rc == 0 && applyArgs.ReplyBuf != nil {
		replySizeRc, copyErr = lso.Pso.CopyDataToBuffer(returnObj, applyArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		return int64(rc)
	}

	return replySizeRc
}

func (lso *LeaseServerObject) leaderInit() {
	for _, leaseObj := range lso.LeaseMap {
		rc := lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
		if rc != 0 {
			log.Error("Unable to get timestamp (InitLeader)")
		}
		leaseObj.TTL = ttlDefault
	}
}

func (lso *LeaseServerObject) peerBootup(userID unsafe.Pointer) {
	readResult, _, _ := lso.Pso.ReadAllKV(userID, "", 0, 0, lso.LeaseColmFam)

	//Result of the read
	for key, value := range readResult {
		//Decode the request structure sent by client.
		lstruct := &leaseLib.LeaseMeta{}
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

func (lso *LeaseServerObject) Init(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	if initPeerArgs.InitState == PumiceDBServer.INIT_BECOMING_LEADER_STATE {
		lso.leaderInit()
	} else if initPeerArgs.InitState == PumiceDBServer.INIT_BOOTUP_STATE {
		lso.peerBootup(initPeerArgs.UserID)
	} else if initPeerArgs.InitState == PumiceDBServer.INIT_BECOMING_CANDIDATE_STATE {
		log.Info("WIP Leader becoming candidate state")
	} else {
		log.Error("Invalid init state: %d", initPeerArgs.InitState)
	}
}
