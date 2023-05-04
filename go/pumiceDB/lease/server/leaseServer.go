package leaseServer

import (
	"bytes"
	leaseLib "common/leaseLib"
	list "container/list"
	"encoding/gob"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"sync"
	"time"
	"unsafe"
)

var ttlDefault = 60
var gcTimeout = 35
var LEASE_COLUMN_FAMILY = "NIOVA_LEASE_CF"

const (
	//Go return codes
	ERROR         int = -1
	SEND_RESPONSE     = 0
	CONTINUE_WR       = 1
)

type LeaseServerObject struct {
	LeaseColmFam string
	LeaseMap     map[uuid.UUID]*leaseLib.LeaseInfo
	Pso          *PumiceDBServer.PmdbServerObject
	listObj      *list.List
	leaseLock    sync.RWMutex
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

func copyToResponse(l *leaseLib.LeaseMeta, r *leaseLib.LeaseRes) {
	r.Resource = l.Resource
	r.Client = l.Client
	r.LeaseState = l.LeaseState
	r.TTL = l.TTL
	r.TimeStamp = l.TimeStamp
}

func (lso *LeaseServerObject) isExpired(ts leaseLib.LeaderTS) bool {
	var ct leaseLib.LeaderTS
	lso.GetLeaderTimeStamp(&ct)
	checkMajorCorrectness(ct.LeaderTerm, ts.LeaderTerm)
	//Check if expired
	if ts.LeaderTime+int64(ttlDefault) <= ct.LeaderTime {
		return true
	}
	return false
}

func (lso *LeaseServerObject) isPermitted(entry *leaseLib.LeaseMeta, clientUUID uuid.UUID, operation int) bool {
	//Check if provided lease operation is permitted for the current lease state
	if (entry.LeaseState == leaseLib.INPROGRESS) || (entry.LeaseState == leaseLib.STALE_INPROGRESS) {
		return false
	}

	//Check if existing lease is valid
	if !lso.isExpired(entry.TimeStamp) {
		// if lease is valid and operation from same client, allow it
		if clientUUID == entry.Client {
			return true
		}
		return false
	} else {
		// If lease expired but refresh allowed from same client only
		if (operation == leaseLib.REFRESH) && (clientUUID == entry.Client) {
			return true
		} else if operation == leaseLib.GET {
			//GET allowed on expired lease from any client
			return true
		}
	}

	return false
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

func (lso *LeaseServerObject) GetLeaderTimeStamp(ts *leaseLib.LeaderTS) error {
	var plts PumiceDBServer.PmdbLeaderTS
	rc := PumiceDBServer.PmdbGetLeaderTimeStamp(&plts)
	ts.LeaderTerm = plts.Term
	ts.LeaderTime = plts.Time
	return rc
}

func (handler *LeaseServerReqHandler) doRefresh() int {

	l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
	if (!isPresent) || (isPresent && !handler.LeaseServerObj.isPermitted(&l.LeaseMetaInfo,
		handler.LeaseReq.Client, handler.LeaseReq.Operation)) {
		return ERROR
	}

	switch l.LeaseMetaInfo.LeaseState {
	case leaseLib.EXPIRED:
		l.LeaseMetaInfo.LeaseState = leaseLib.INPROGRESS
		return CONTINUE_WR

	case leaseLib.EXPIRED_LOCALLY:
		fallthrough

	case leaseLib.GRANTED:
		l.LeaseMetaInfo.LeaseState = leaseLib.GRANTED

		//Refresh the lease
		var currentTime leaseLib.LeaderTS
		handler.LeaseServerObj.GetLeaderTimeStamp(&currentTime)
		l.LeaseMetaInfo.TimeStamp = currentTime
		l.LeaseMetaInfo.TTL = ttlDefault

		//Copy the encoded result in replyBuffer
		copyToResponse(&l.LeaseMetaInfo, handler.LeaseRes)

		//List order sanity check
		if handler.LeaseServerObj.listObj.Back() != nil {
			listOrderSanityCheck(handler.LeaseServerObj.listObj.Back().Value.(*leaseLib.LeaseInfo), l)
		}

		//Update lease list
		handler.LeaseServerObj.listObj.MoveToBack(l.ListElement)
		//Response from prepare itself
		return SEND_RESPONSE
	default:
		log.Error("Unexpected lease state encountered in refresh : ", l.LeaseMetaInfo.LeaseState)
	}
	return ERROR
}

func (lso *LeaseServerObject) prepare(request leaseLib.LeaseReq, response *leaseLib.LeaseRes) int {
	var ct leaseLib.LeaderTS
	lso.GetLeaderTimeStamp(&ct)

	//Obtain lock
	lso.leaseLock.Lock()
	defer lso.leaseLock.Unlock()

	//Switch
	switch request.Operation {
	case leaseLib.STALE_REMOVAL:
		if request.InitiatorTerm == ct.LeaderTerm {
			log.Trace("Request for Stale lease processing from same term")
			return CONTINUE_WR
		}
		log.Trace("GC request from previous term encountered")
		return ERROR

	case leaseLib.REFRESH:

		leaseReq := LeaseServerReqHandler{
			LeaseServerObj: lso,
			LeaseReq:       request,
			LeaseRes:       response,
		}
		return leaseReq.doRefresh()

	case leaseLib.GET:
		vdev_lease_info, isPresent := lso.LeaseMap[request.Resource]
		if (isPresent) && (!lso.isPermitted(&vdev_lease_info.LeaseMetaInfo, request.Client, request.Operation)) {
			//Dont provide lease
			return ERROR
		}

		var li leaseLib.LeaseInfo
		//Insert or update into MAP
		li.LeaseMetaInfo.Resource = request.Resource
		li.LeaseMetaInfo.Client = request.Client
		li.LeaseMetaInfo.LeaseState = leaseLib.INPROGRESS
		lso.LeaseMap[request.Resource] = &li
		return CONTINUE_WR
	default:
		log.Error("Invalid operation", request.Operation)
		return ERROR
	}

	return ERROR

}

func (lso *LeaseServerObject) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {
	var e error
	var ret int64

	//Decode request
	rq, err := Decode(wrPrepArgs.Payload)
	if err != nil {
		log.Error(err)
		return -1
	}

	var rs leaseLib.LeaseRes
	rc := lso.prepare(rq, &rs)

	switch rc {
	case ERROR:
		_, e = lso.Pso.CopyDataToBuffer(byte(0), wrPrepArgs.ContinueWr)
		if e != nil {
			log.Error("Failed to Copy result in the buffer: %s", e)
		}
		return -1

	case SEND_RESPONSE:
		_, e = lso.Pso.CopyDataToBuffer(byte(0), wrPrepArgs.ContinueWr)
		if e != nil {
			log.Error("Failed to Copy result in the buffer: %s", e)
			return 0
		}

		ret, e = lso.Pso.CopyDataToBuffer(rs, wrPrepArgs.ReplyBuf)
		if e != nil {
			log.Error("Failed to Copy result in the buffer: %s", e)
			return 0
		}
		return ret

	case CONTINUE_WR:
		_, e = lso.Pso.CopyDataToBuffer(byte(1), wrPrepArgs.ContinueWr)
		if e != nil {
			log.Error("Failed to Copy result in the buffer: %s", e)
			return 0
		}
		return 0
	}

	return 0
}

func (handler *LeaseServerReqHandler) readLease() int {
	handler.LeaseServerObj.leaseLock.Lock()
	defer handler.LeaseServerObj.leaseLock.Unlock()

	switch handler.LeaseReq.Operation {
	case leaseLib.LOOKUP:
		l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
		if !isPresent {
			return ERROR
		}
		copyToResponse(&l.LeaseMetaInfo, handler.LeaseRes)

	case leaseLib.LOOKUP_VALIDATE:
		l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
		if (!isPresent) || (isPresent && l.LeaseMetaInfo.LeaseState == leaseLib.INPROGRESS) {
			return ERROR
		}

		lso := handler.LeaseServerObj
		if l.LeaseMetaInfo.LeaseState == leaseLib.GRANTED {
			if handler.LeaseServerObj.isExpired(l.LeaseMetaInfo.TimeStamp) {
				l.LeaseMetaInfo.TTL = 0
				l.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED_LOCALLY
			} else {
				var ct leaseLib.LeaderTS
				lso.GetLeaderTimeStamp(&ct)
				l.LeaseMetaInfo.TTL = int(l.LeaseMetaInfo.TimeStamp.LeaderTime - ct.LeaderTime + int64(l.LeaseMetaInfo.TTL))
			}
		}
		copyToResponse(&l.LeaseMetaInfo, handler.LeaseRes)

	default:
		log.Error("Unkown read lease operation ", handler.LeaseReq.Operation)
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

	//Prepare request obj
	leaseReq := LeaseServerReqHandler{
		LeaseServerObj: lso,
		LeaseReq:       reqStruct,
		LeaseRes:       &returnObj,
	}
	//Read lease
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

func listOrderSanityCheck(fe *leaseLib.LeaseInfo, se *leaseLib.LeaseInfo) {
	log.Trace("Sanity checks for list order")
	if (fe != nil) && (se != nil) {
		exp_fe := fe.LeaseMetaInfo.TimeStamp.LeaderTime
		exp_se := se.LeaseMetaInfo.TimeStamp.LeaderTime
		if exp_fe > exp_se {
			log.Fatal("(Lease server) List ordering issue  detected")
		}
	}
}

func (handler *LeaseServerReqHandler) initLease() (*leaseLib.LeaseInfo, error) {
	var lo leaseLib.LeaseInfo
	var lop *leaseLib.LeaseInfo
	handler.LeaseServerObj.leaseLock.Lock()
	lop, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
	if !isPresent {
		lop = &lo
		lop.LeaseMetaInfo.Resource = handler.LeaseReq.Resource
		lop.LeaseMetaInfo.Client = handler.LeaseReq.Client
		handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource] = lop
	}
	err := handler.LeaseServerObj.GetLeaderTimeStamp(&lop.LeaseMetaInfo.TimeStamp)
	lop.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
	lop.LeaseMetaInfo.TTL = ttlDefault

	//Insert into list
	lop.ListElement = &list.Element{}
	lop.ListElement.Value = lop

	//Any apply requires lease to be pushed back in the list
	if (handler.LeaseServerObj.listObj.Back() != nil) && (err == nil) {
		listOrderSanityCheck(handler.LeaseServerObj.listObj.Back().Value.(*leaseLib.LeaseInfo), lop)
	}
	handler.LeaseServerObj.listObj.PushBack(lop)
	handler.LeaseServerObj.leaseLock.Unlock()

	return lop, err
}

func (handler *LeaseServerReqHandler) applyLease() int {

	lop, isleader := handler.initLease()

	copyToResponse(&lop.LeaseMetaInfo, handler.LeaseRes)
	valueBytes := bytes.Buffer{}
	enc := gob.NewEncoder(&valueBytes)
	err := enc.Encode(lop.LeaseMetaInfo)
	if err != nil {
		log.Error(err)
		return 1
	}

	byteToStr := string(valueBytes.Bytes())

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

	if isleader == nil {
		copyToResponse(&lop.LeaseMetaInfo, handler.LeaseRes)
		return 0
	}
	return 1
}

func (handler *LeaseServerReqHandler) gcReqHandler() {
	for i := 0; i < len(handler.LeaseReq.Resources); i++ {
		//Set lease as expired
		resource := handler.LeaseReq.Resources[i]

		//Obtain lock
		handler.LeaseServerObj.leaseLock.Lock()

		//Update lease in map
		lease, isPresent := handler.LeaseServerObj.LeaseMap[resource]
		if (!isPresent) || (isPresent && lease.LeaseMetaInfo.LeaseState != leaseLib.STALE_INPROGRESS) {
			log.Error("GCed resource is not present or have modified state", resource, lease.LeaseMetaInfo.LeaseState)
			handler.LeaseServerObj.leaseLock.Unlock()
			continue
		}

		//Set lease as expired
		lease.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED
		lease.LeaseMetaInfo.TTL = 0

		//Remove from list
		handler.LeaseServerObj.listObj.Remove(lease.ListElement)

		//Write to RocksDB
		valueBytes := bytes.Buffer{}
		enc := gob.NewEncoder(&valueBytes)
		err := enc.Encode(lease.LeaseMetaInfo)
		if err != nil {
			log.Error(err)
			handler.LeaseServerObj.leaseLock.Unlock()
			continue
		}
		handler.LeaseServerObj.leaseLock.Unlock()

		byteToStr := string(valueBytes.Bytes())
		valLen := len(byteToStr)
		rc := handler.LeaseServerObj.Pso.WriteKV(handler.UserID, handler.PmdbHandler,
			resource.String(), int64(len(resource.String())), byteToStr, int64(valLen),
			handler.LeaseServerObj.LeaseColmFam)
		if rc < 0 {
			log.Error("Expired lease update to RocksDB failed")
		}
	}
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

	log.Trace("Apply for operation: ", applyLeaseReq.Operation)

	//Handle client request
	var returnObj leaseLib.LeaseRes
	leaseReq := LeaseServerReqHandler{
		LeaseServerObj: lso,
		LeaseReq:       applyLeaseReq,
		LeaseRes:       &returnObj,
		UserID:         applyArgs.UserID,
		PmdbHandler:    applyArgs.PmdbHandler,
	}

	//Handle GC request
	switch applyLeaseReq.Operation {
	case leaseLib.STALE_REMOVAL:
		leaseReq.gcReqHandler()
		return 0
	case leaseLib.REFRESH:
		fallthrough
	case leaseLib.GET:
		rc := leaseReq.applyLease()
		if !((rc == 0) && (applyArgs.ReplyBuf != nil)) {
			return int64(rc)
		}
		replySizeRc, copyErr = lso.Pso.CopyDataToBuffer(returnObj,
			applyArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return int64(ERROR)
		}
		return replySizeRc
	default:
		log.Error("Unkown lease operation identified in Apply callback : ", applyLeaseReq.Operation)
	}

	return int64(ERROR)
}

func (lso *LeaseServerObject) leaderInit() {
	for e := lso.listObj.Front(); e != nil; e = e.Next() {
		if lo, ok := e.Value.(*leaseLib.LeaseInfo); ok {
			if lo.LeaseMetaInfo.LeaseState != leaseLib.EXPIRED {
				rc := lso.GetLeaderTimeStamp(&lo.LeaseMetaInfo.TimeStamp)
				if rc != nil {
					log.Error("Unable to get timestamp (InitLeader)")
				}
				lo.LeaseMetaInfo.TTL = ttlDefault
				lo.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
			}
		}
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
		if leaseInfo.LeaseMetaInfo.LeaseState != leaseLib.EXPIRED {
			leaseInfo.ListElement = &list.Element{}
			leaseInfo.ListElement.Value = &leaseInfo
			lso.listObj.PushBack(&leaseInfo)
		}
		delete(readResult, key)
	}
}

func (lso *LeaseServerObject) sendGCReq(resourceUUIDs []uuid.UUID, leaderTerm int64) {
	//Send GC request if only expired lease found
	if len(resourceUUIDs) > 0 {
		//Create request
		var r leaseLib.LeaseReq
		r.Operation = leaseLib.STALE_REMOVAL
		r.Resources = resourceUUIDs
		r.InitiatorTerm = leaderTerm
		//Send Request
		log.Trace("Send stale lease processing request")
		err := PumiceDBServer.PmdbEnqueueDirectWriteRequest(r)
		if err != nil {
			log.Error("Failed to send stale lease processing request")
		}
	}
}

func (lso *LeaseServerObject) leaseGarbageCollector() {
	//Init ticker
	t := time.NewTicker(time.Duration(gcTimeout) * time.Second)
	log.Info("GC routine running")
	for {
		<-t.C
		var ct leaseLib.LeaderTS
		err := lso.GetLeaderTimeStamp(&ct)
		if err != nil {
			continue
		}
		var rUUIDs []uuid.UUID
		stopScan := false

		//Obtain lock
		lso.leaseLock.Lock()
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
				if lso.isExpired(obj.TimeStamp) {
					cobj.LeaseMetaInfo.LeaseState = leaseLib.STALE_INPROGRESS
					log.Trace("Enqueue lease for stale lease processing: ",
						cobj.LeaseMetaInfo.Resource, cobj.LeaseMetaInfo.Client)
					rUUIDs = append(rUUIDs, obj.Resource)
				} else {
					log.Trace("GC scanning finished")
					stopScan = true
				}
			}
			//no more expired leases.
			if stopScan {
				break
			}
		}
		//Release lock
		lso.leaseLock.Unlock()

		//Send GC request if only expired lease found
		lso.sendGCReq(rUUIDs, ct.LeaderTerm)
	}
}

func (lso *LeaseServerObject) Init(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	switch initPeerArgs.InitState {
	case PumiceDBServer.INIT_BECOMING_LEADER_STATE:
		log.Info("Init callback in peer becoming leader.")
		lso.leaderInit()
	case PumiceDBServer.INIT_BOOTUP_STATE:
		log.Info("Init callback on peer bootup.")
		lso.peerBootup(initPeerArgs.UserID)
	case PumiceDBServer.INIT_BECOMING_CANDIDATE_STATE:
		log.Info("Init callback on peer becoming candidate.")
	default:
		log.Error("Invalid init state: %d", initPeerArgs.InitState)
	}
}
