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
	LeaseMap     map[uuid.UUID]*leaseLib.LeaseInfo
	Pso          *PumiceDBServer.PmdbServerObject
	listObj      *list.List
}

type LeaseServerReqHandler struct {
	LeaseServerObj *LeaseServerObject
	LeaseReq       leaseLib.LeaseReq
	LeaseRes       *leaseLib.LeaseRes
	UserID         unsafe.Pointer
	PmdbHandler    unsafe.Pointer
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
	lso.LeaseMap = make(map[uuid.UUID]*leaseLib.LeaseInfo)
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
			if isPermitted(&vdev_lease_info.LeaseMetaInfo, request.Client,
				currentTime, request.Operation) {
				//Refresh the lease
				lso.LeaseMap[request.Resource].LeaseMetaInfo.TimeStamp = currentTime
				lso.LeaseMap[request.Resource].LeaseMetaInfo.TTL = ttlDefault
				//Copy the encoded result in replyBuffer
				//TODO: Pointer leak from list element
				copyToResponse(&lso.LeaseMap[request.Resource].LeaseMetaInfo, reply)
				//Update lease list
				lso.listObj.MoveToBack(lso.LeaseMap[request.Resource].ListElement)
				return 0
			}
		}
		return -1
	} else if request.Operation == leaseLib.GET {
		vdev_lease_info, isPresent := lso.LeaseMap[request.Resource]
		if isPresent {
			if !isPermitted(&vdev_lease_info.LeaseMetaInfo, request.Client,
				currentTime, request.Operation) {
				//Dont provide lease
				return -1
			}
		}
		var leaseInfo leaseLib.LeaseInfo
		//Insert or update into MAP
		leaseInfo.LeaseMetaInfo.Resource = request.Resource
		leaseInfo.LeaseMetaInfo.Client = request.Client
		leaseInfo.LeaseMetaInfo.LeaseState = leaseLib.INPROGRESS
		lso.LeaseMap[request.Resource] = &leaseInfo
	} else {
		log.Error("Invalid operation", request.Operation)
		return -1
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

func (handler *LeaseServerReqHandler) readLease() int {
	if handler.LeaseReq.Operation == leaseLib.LOOKUP {
		leaseObj, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
		if !isPresent {
			return -1
		}
		leaseObj.LeaseMetaInfo.TTL = -1
		leaseObj.LeaseMetaInfo.LeaseState = leaseLib.NULL
		handler.LeaseServerObj.GetLeaderTimeStamp(&leaseObj.LeaseMetaInfo.TimeStamp)
		copyToResponse(&leaseObj.LeaseMetaInfo, handler.LeaseRes)

	} else if handler.LeaseReq.Operation == leaseLib.LOOKUP_VALIDATE {
		leaseObj, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
		if !isPresent {
			return -1
		}

		if leaseObj.LeaseMetaInfo.LeaseState == leaseLib.INPROGRESS {
			return -1
		}

		oldTS := leaseObj.LeaseMetaInfo.TimeStamp
		handler.LeaseServerObj.GetLeaderTimeStamp(&leaseObj.LeaseMetaInfo.TimeStamp)
		checkMajorCorrectness(leaseObj.LeaseMetaInfo.TimeStamp.LeaderTerm, oldTS.LeaderTerm)

		//TODO: Possible wrap around
		ttl := leaseObj.LeaseMetaInfo.TTL - int(leaseObj.LeaseMetaInfo.TimeStamp.LeaderTime-oldTS.LeaderTime)
		if ttl < 0 {
			leaseObj.LeaseMetaInfo.TTL = 0
			leaseObj.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED
		} else {
			leaseObj.LeaseMetaInfo.TTL = ttl
		}
		//*reply = *leaseObj
		handler.LeaseRes.Client = leaseObj.LeaseMetaInfo.Client
		handler.LeaseRes.Resource = leaseObj.LeaseMetaInfo.Resource
		handler.LeaseRes.Status = leaseObj.LeaseMetaInfo.Status
		handler.LeaseRes.LeaseState = leaseObj.LeaseMetaInfo.LeaseState
		handler.LeaseRes.TTL = leaseObj.LeaseMetaInfo.TTL
		handler.LeaseRes.TimeStamp = leaseObj.LeaseMetaInfo.TimeStamp
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

	var returnObj leaseLib.LeaseRes
	var replySize int64
	var copyErr error

	leaseReq := LeaseServerReqHandler{
		LeaseServerObj: lso,
		LeaseReq:       reqStruct,
		LeaseRes:       &returnObj,
	}

	//rc := lso.readLease(reqStruct, &returnObj)
	rc := leaseReq.readLease()

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

func (handler *LeaseServerReqHandler) applyLease() int {
	var leaseObj leaseLib.LeaseInfo
	var leaseObjPtr *leaseLib.LeaseInfo

	leaseObjPtr, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
	if !isPresent {
		leaseObjPtr = &leaseObj
		handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource] = leaseObjPtr
	}

	leaseObjPtr.LeaseMetaInfo.Resource = handler.LeaseReq.Resource
	leaseObjPtr.LeaseMetaInfo.Client = handler.LeaseReq.Client
	leaseObjPtr.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
	isLeaderFlag := handler.LeaseServerObj.GetLeaderTimeStamp(&leaseObjPtr.LeaseMetaInfo.TimeStamp)
	leaseObjPtr.LeaseMetaInfo.TTL = ttlDefault

	//Insert into list
	leaseObjPtr.ListElement = &list.Element{}
	leaseObjPtr.ListElement.Value = leaseObjPtr
	handler.LeaseServerObj.listObj.PushBack(leaseObjPtr)

	valueBytes := bytes.Buffer{}
	//gob.Register(leaseLib.LeaseStruct{})
	enc := gob.NewEncoder(&valueBytes)
	err := enc.Encode(leaseObjPtr.LeaseMetaInfo)
	if err != nil {
		log.Error(err)
		return 1
	}

	byteToStr := string(valueBytes.Bytes())
	log.Trace("Value passed by client: ", byteToStr)

	// Length of value.
	valLen := len(byteToStr)
	keyLength := len(handler.LeaseReq.Resource.String())
	rc := handler.LeaseServerObj.Pso.WriteKV(handler.UserID, handler.PmdbHandler, handler.LeaseReq.Resource.String(), int64(keyLength), byteToStr, int64(valLen), handler.LeaseServerObj.LeaseColmFam)
	if rc < 0 {
		leaseObjPtr.LeaseMetaInfo.Status = "Key not found"
		log.Error("Value not written to rocksdb")
		return -1
	} else {
		leaseObjPtr.LeaseMetaInfo.Status = "Success"
	}

	if isLeaderFlag == 0 {
		//*reply = *leaseObj
		handler.LeaseRes.Client = leaseObjPtr.LeaseMetaInfo.Client
		handler.LeaseRes.Resource = leaseObjPtr.LeaseMetaInfo.Resource
		handler.LeaseRes.Status = leaseObjPtr.LeaseMetaInfo.Status
		handler.LeaseRes.LeaseState = leaseObjPtr.LeaseMetaInfo.LeaseState
		handler.LeaseRes.TTL = leaseObjPtr.LeaseMetaInfo.TTL
		handler.LeaseRes.TimeStamp = leaseObjPtr.LeaseMetaInfo.TimeStamp
		return 0
	}

	//Check if lease is added properly
	_, isPresent1 := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]

	if !isPresent1 {
		log.Info("Just added lease but not present in map!")
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
	leaseReq := LeaseServerReqHandler{
		LeaseServerObj: lso,
		LeaseReq:       applyLeaseReq,
		LeaseRes:       &returnObj,
		UserID:         applyArgs.UserID,
		PmdbHandler:    applyArgs.PmdbHandler,
	}
	rc := leaseReq.applyLease()
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
		rc := lso.GetLeaderTimeStamp(&leaseObj.LeaseMetaInfo.TimeStamp)
		if rc != 0 {
			log.Error("Unable to get timestamp (InitLeader)")
		}
		leaseObj.LeaseMetaInfo.TTL = ttlDefault
	}
}

func (lso *LeaseServerObject) peerBootup(userID unsafe.Pointer) {
	readResult, _, _ := lso.Pso.ReadAllKV(userID, "", 0, 0, lso.LeaseColmFam)

	//Result of the read
	for key, value := range readResult {
		//Decode the request structure sent by client.
		var leaseInfo leaseLib.LeaseInfo
		dec := gob.NewDecoder(bytes.NewBuffer(value))
		decodeErr := dec.Decode(&leaseInfo.LeaseMetaInfo)
		if decodeErr != nil {
			log.Error("Failed to decode the read request : ", decodeErr)
			return
		}
		kuuid, _ := uuid.FromString(key)
		lso.LeaseMap[kuuid] = &leaseInfo
		delete(readResult, key)
	}
}

func (lso *LeaseServerObject) Init(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	if initPeerArgs.InitState == PumiceDBServer.INIT_BECOMING_LEADER_STATE {
		log.Info("Init callback in peer becoming leader.")
		lso.leaderInit()
	} else if initPeerArgs.InitState == PumiceDBServer.INIT_BOOTUP_STATE {
		log.Info("Init callback on peer bootup.")
		lso.peerBootup(initPeerArgs.UserID)
	} else if initPeerArgs.InitState == PumiceDBServer.INIT_BECOMING_CANDIDATE_STATE {
		log.Info("Init callback on peer becoming candidate.")
	} else {
		log.Error("Invalid init state: %d", initPeerArgs.InitState)
	}
}
