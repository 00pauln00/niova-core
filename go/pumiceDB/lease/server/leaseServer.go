package leaseServer

import (
	"bytes"
	leaseLib "common/leaseLib"
	list "container/list"
	"encoding/gob"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"sync"
	"time"
	"unsafe"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

var ttlDefault = 60
var gcTimeout = 35
var LEASE_COLUMN_FAMILY = "NIOVA_LEASE_CF"

type LeaseServerObject struct {
	LeaseColmFam string
	LeaseMap     map[uuid.UUID]*leaseLib.LeaseInfo
	Pso          *PumiceDBServer.PmdbServerObject
	listObj      *list.List
	listLock     sync.RWMutex
}

type LeaseServerReqHandler struct {
	LeaseServerObj *LeaseServerObject
	LeaseReq       leaseLib.LeaseReq
	LeaseRes       *leaseLib.LeaseRes
	UserID         unsafe.Pointer
	PmdbHandler    unsafe.Pointer
}

//Helper functions
func checkMajorCorrectness(cTerm int64, lTerm int64) {
	if cTerm != lTerm {
		log.Fatal("Major(Term) not matching")
	}
}


func isPermitted(entry *leaseLib.LeaseMeta, clientUUID uuid.UUID, currentTime leaseLib.LeaderTS, operation int) bool {
	if (entry.LeaseState == leaseLib.INPROGRESS) || (entry.LeaseState == leaseLib.STALE_INPROGRESS) {
		return false
	}
	le := entry.TimeStamp.LeaderTime + int64(entry.TTL)

	//Check majot correctness
	checkMajorCorrectness(currentTime.LeaderTerm, entry.TimeStamp.LeaderTerm)
	//Check if existing lease is valid; by comparing only the minor
	sv := le > currentTime.LeaderTime
	//If valid, Check if client uuid is same and operation is refresh
	if sv {
		if (operation == leaseLib.REFRESH) && (clientUUID == entry.Client) {
			return true
		} else {
			return false
		}
	}
	return true
}

func Decode(payload []byte) (leaseLib.LeaseReq, error) {
	r := &leaseLib.LeaseReq{}
	dec := gob.NewDecoder(bytes.NewBuffer(payload))
	err := dec.Decode(r)
	return *r, err
}

func (lso *LeaseServerObject) InitLeaseObject(pso *PumiceDBServer.PmdbServerObject) {
	lso.Pso = pso
	lso.LeaseMap = make(map[uuid.UUID]*leaseLib.LeaseInfo)
	lso.LeaseColmFam = LEASE_COLUMN_FAMILY
	//Register Lease callbacks
	lso.Pso.LeaseAPI = lso
	lso.listObj = list.New()
	//Start the garbage collector thread
	go lso.leaseGarbageCollector()
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

	if request.Operation == leaseLib.GC {
		if request.InitiatorTerm == currentTime.LeaderTerm {
			log.Info("Request for Stale lease processing from same term")
			return 1
		}
		log.Info("GC request from previous term encountered")
		return -1
	}

	//Check if its a refresh request
	if request.Operation == leaseLib.REFRESH {
		vdev_lease_info, isPresent := lso.LeaseMap[request.Resource]
		if (isPresent) && (vdev_lease_info.LeaseMetaInfo.LeaseState != leaseLib.EXPIRED) {
			if isPermitted(&vdev_lease_info.LeaseMetaInfo, request.Client, currentTime, request.Operation) {
				//Refresh the lease
				lso.LeaseMap[request.Resource].LeaseMetaInfo.TimeStamp = currentTime
				lso.LeaseMap[request.Resource].LeaseMetaInfo.TTL = ttlDefault
				//Copy the encoded result in replyBuffer
				copyToResponse(lso.LeaseMap[request.Resource].LeaseMetaInfo, reply)
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
	lso.listLock.Lock()
	rc := lso.prepare(request, &returnObj)
	lso.listLock.Unlock()

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

func copyToResponse(l leaseLib.LeaseMeta, r *leaseLib.LeaseRes) {
	r.Client = l.Client
	r.Resource = l.Resource
	r.Status = l.Status
	r.LeaseState = l.LeaseState
	r.TTL = l.TTL
	r.TimeStamp = l.TimeStamp
}

func (handler *LeaseServerReqHandler) readLease() int {
	if handler.LeaseReq.Operation == leaseLib.LOOKUP {
		l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
		if !isPresent {
			return -1
		}
		l.LeaseMetaInfo.TTL = -1
		l.LeaseMetaInfo.LeaseState = leaseLib.NULL
		handler.LeaseServerObj.GetLeaderTimeStamp(&l.LeaseMetaInfo.TimeStamp)
		copyToResponse(l.LeaseMetaInfo, handler.LeaseRes)

	} else if handler.LeaseReq.Operation == leaseLib.LOOKUP_VALIDATE {
		l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
		if !isPresent {
			return -1
		}

		if l.LeaseMetaInfo.LeaseState == leaseLib.INPROGRESS {
			return -1
		}

		var currentTime leaseLib.LeaderTS
		handler.LeaseServerObj.GetLeaderTimeStamp(&currentTime)
		checkMajorCorrectness(currentTime.LeaderTerm, l.LeaseMetaInfo.TimeStamp.LeaderTerm)

		//Take the lock as we are modifying lease parameters here.
		lso := handler.LeaseServerObj
		lso.listLock.Lock()
		if l.LeaseMetaInfo.LeaseState == leaseLib.GRANTED {
			//TODO: Possible wrap around
			ttl := l.LeaseMetaInfo.TTL -
				int(currentTime.LeaderTime-l.LeaseMetaInfo.TimeStamp.LeaderTime)
			if ttl < 0 {
				log.Trace("Lookup validate marking lease as expired locally ",
						l.LeaseMetaInfo.Resource, l.LeaseMetaInfo.Client)
				l.LeaseMetaInfo.TTL = 0
				l.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED_LOCALLY
			} else {
				l.LeaseMetaInfo.TTL = ttl
			}
		}
		lso.listLock.Unlock()

		copyToResponse(l.LeaseMetaInfo, handler.LeaseRes)
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
	var lop *leaseLib.LeaseInfo

	lop, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
	if !isPresent {
		lop := &leaseLib.LeaseInfo{}
		handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource] = lop
	}

	lop.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
	lop.LeaseMetaInfo.TTL = ttlDefault
	isLeaderFlag := handler.LeaseServerObj.GetLeaderTimeStamp(&lop.LeaseMetaInfo.TimeStamp)

	copyToResponse(lop.LeaseMetaInfo, handler.LeaseRes)

	//Insert into list
	lop.ListElement = &list.Element{}
	lop.ListElement.Value = lop
	handler.LeaseServerObj.listObj.PushBack(lop)

	valueBytes := bytes.Buffer{}
	enc := gob.NewEncoder(&valueBytes)
	err := enc.Encode(lop.LeaseMetaInfo)
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
		lop.LeaseMetaInfo.Status = leaseLib.FAILURE
		log.Error("Value not written to rocksdb")
		return -1
	} else {
		lop.LeaseMetaInfo.Status = leaseLib.SUCCESS
	}

	if isLeaderFlag == 0 {
		copyToResponse(lop.LeaseMetaInfo, handler.LeaseRes)
		return 0
	}

	return 1
}

func (handler *LeaseServerReqHandler) gcReqHandler() {
	lso := handler.LeaseServerObj
	for i := 0; i < len(handler.LeaseReq.Resources); i++ {
		//Take the lock as we are marking lease state as expired.
		lso.listLock.Lock()
		r := handler.LeaseReq.Resources[i]
		l := handler.LeaseServerObj.LeaseMap[r]
		l.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED
		l.LeaseMetaInfo.TTL = 0

		log.Trace("Marking lease expired: ", l.LeaseMetaInfo.Resource, l.LeaseMetaInfo.Client)
		lso.listLock.Unlock()

		//Write to RocksDB
		valueBytes := bytes.Buffer{}
		enc := gob.NewEncoder(&valueBytes)
		err := enc.Encode(l.LeaseMetaInfo)
		if err != nil {
			log.Error(err)
			break
		}
		byteToStr := string(valueBytes.Bytes())
		valLen := len(byteToStr)
		keyLen := len(r.String())
		
		handler.LeaseServerObj.Pso.WriteKV(handler.UserID, handler.PmdbHandler,
			r.String(), int64(keyLen), byteToStr, int64(valLen),
			handler.LeaseServerObj.LeaseColmFam)
	}
}

func (lso *LeaseServerObject) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {
	var copyErr error
	var replySizeRc int64

	// Decode the input buffer into structure format
	applyLeaseReq, err := Decode(applyArgs.Payload)
	if err != nil {
		log.Error(err)
		return -1
	}

	log.Trace("Apply for operation: ", applyLeaseReq.Operation)

	//Handle client request
	var returnObj leaseLib.LeaseRes
	lR := LeaseServerReqHandler{
		LeaseServerObj: lso,
		LeaseReq:       applyLeaseReq,
		LeaseRes:       &returnObj,
		UserID:         applyArgs.UserID,
		PmdbHandler:    applyArgs.PmdbHandler,
	}

	//Handle GC request
	if applyLeaseReq.Operation == leaseLib.GC {
		lR.gcReqHandler()
		return 0
	}

	//Handler GET lease request
	rc := lR.applyLease()
	log.Trace("(Apply) Lease request by client : ", applyLeaseReq.Client.String(), " for resource : ", applyLeaseReq.Resource.String())
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
	for _, lO := range lso.LeaseMap {
		rc := lso.GetLeaderTimeStamp(&lO.LeaseMetaInfo.TimeStamp)
		if rc != 0 {
			log.Error("Unable to get timestamp (InitLeader)")
		}
		lO.LeaseMetaInfo.TTL = ttlDefault
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
		leaseInfo.ListElement = &list.Element{}
		leaseInfo.ListElement.Value = &leaseInfo
		lso.listObj.PushBack(leaseInfo)
		delete(readResult, key)
	}
}

func (lso *LeaseServerObject) leaseGarbageCollector() {
	ticker := time.NewTicker(time.Duration(gcTimeout) * time.Second)
	log.Info("GC routine running")
	for {
		<-ticker.C
		var cT leaseLib.LeaderTS
		err := lso.GetLeaderTimeStamp(&cT)
		if err != 0 {
			continue
		}
		var rUUIDs []uuid.UUID
		//Obtain lock
		lso.listLock.Lock()
		//Iterate over list
		for e := lso.listObj.Front(); e != nil; e = e.Next() {
			if cobj, ok := e.Value.(*leaseLib.LeaseInfo); ok {
				//Check lease status before processing it.
				obj := cobj.LeaseMetaInfo
				if obj.LeaseState != leaseLib.GRANTED &&
					obj.LeaseState != leaseLib.STALE_INPROGRESS &&
					obj.LeaseState != leaseLib.EXPIRED_LOCALLY {
					continue
				}
				err = lso.GetLeaderTimeStamp(&cT)
				if err != 0 {
					//Leader change, so stop the routine execution
					break
				}
				lt := obj.TimeStamp.LeaderTime
				ttl := ttlDefault - int(cT.LeaderTime-lt)
				if ttl <= 0 {
					cobj.LeaseMetaInfo.LeaseState = leaseLib.STALE_INPROGRESS
					log.Trace("Enqueue lease for stale lease processing: ",
							cobj.LeaseMetaInfo.Resource, cobj.LeaseMetaInfo.Client)
					rUUIDs = append(rUUIDs, obj.Resource)
				} else {
					break
				}
			}
		}
		//Collect till expired
		lso.listLock.Unlock()
		
		//Send request only if stale lease are present
		if len(rUUIDs) > 0 {
			//Create request
			var r leaseLib.LeaseReq
			r.Operation = leaseLib.GC
			r.Resources = rUUIDs
			r.InitiatorTerm = cT.LeaderTerm
			//Send Request
			log.Trace("Send stale lease processing request")
			err := PumiceDBServer.PmdbEnqueueDirectWriteRequest(r)
			if err != nil {
				log.Error("Failed to send stale lease processing request")
			}
		}
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
