package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"foodpalaceapp.com/foodpalaceapplib"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"niova/go-pumicedb-lib/client"
	"niova/go-pumicedb-lib/common"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var (
	raftUuid     string
	clientUuid   string
	jsonOutFpath string
	data         map[string]map[string]string
)

// Creating an interface for client.
type FoodpalaceCli interface {
	// Methods
	prepare() error
	exec() error
	complete() error
}

//Structure definition to dump into json.
type foodpalaceRqOp struct {
	Operation string
	Status    int
	Timestamp string
	Data      map[string]map[string]string
}

//Structure definition to store request specific information.
type rqInfo struct {
	key            string
	rncui          string
	operation      string
	status         int
	jsonFname      string
	outfileUuid    string
	clientObj      *PumiceDBClient.PmdbClientObj
	foodpalaceData *foodpalaceapplib.FoodpalaceData
	outfilename    string
}

//Structure definition for writeOne operation.
type writeOne struct {
	args []string
	rq   *rqInfo
}

//Structure definition for writeMulti operation.
type writeMulti struct {
	csvFpath     string
	rq           *rqInfo
	multiReqdata []*foodpalaceapplib.FoodpalaceData
}

//Structure definition for readOne operation.
type readOne struct {
	rq *rqInfo
}

//Structure definition for readMulti operation.
type readMulti struct {
	rq      *rqInfo
	rmRncui []string
	rmData  []*foodpalaceapplib.FoodpalaceData
}

//Structure definition for getLeader operation.
type getLeader struct {
	rq       *rqInfo
	pmdbInfo *PumiceDBCommon.PMDBInfo
}

//Function to initialize logger.
func initLogger() {

	var filename string = jsonOutFpath + "/" + clientUuid + ".log"
	// Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Formatter.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		fmt.Println(err)
	} else {
		log.SetOutput(f)
	}
	log.Info("client uuid:", clientUuid)
}

//Function to get current time.
func getCurrentTime() string {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	return timestamp
}

//Method to parse csv file and fill the required structures for writemulti operation.
func (wm *writeMulti) getWmInfo() []*foodpalaceapplib.FoodpalaceData {

	var multireqDt []*foodpalaceapplib.FoodpalaceData
	//Open the file.
	csvfile, err := os.Open(wm.csvFpath)
	if err != nil {
		log.Error("Couldn't open the csv file", err)
	}
	//Parse the file, Skip first row (line)
	row1, err := bufio.NewReader(csvfile).ReadSlice('\n')
	if err != nil {
		log.Error("error")
	}
	_, err = csvfile.Seek(int64(len(row1)), io.SeekStart)
	if err != nil {
		log.Error("error")
	}
	//Read remaining rows.
	r := csv.NewReader(csvfile)
	//Iterate through the records.
	for {
		//Read each record from csv.
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		//Typecast RestaurantId to int64.
		restIdStr, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			log.Error("Error occured in typecasting RestaurantId to int64")
		}
		//Typecast Votes to int64.
		votesStr, err := strconv.ParseInt(record[5], 10, 64)
		if err != nil {
			log.Error("Error occured in typecasting Votes to int64")
		}

		//Fill the Foodpalace App structure.
		zmDt := foodpalaceapplib.FoodpalaceData{
			RestaurantId:   restIdStr,
			RestaurantName: record[1],
			City:           record[2],
			Cuisines:       record[3],
			RatingsText:    record[4],
			Votes:          votesStr,
		}
		multireqDt = append(multireqDt, &zmDt)
	}
	return multireqDt
}

//Method to display output of read one operation and fill the structure.
func (dro *readOne) displayAndFill(rdData *foodpalaceapplib.FoodpalaceData) {

	restId := strconv.Itoa(int(rdData.RestaurantId))
	restVotes := strconv.Itoa(int(rdData.Votes))
	log.Info("Result of the read request is:", rdData)
	rdReqMp := map[string]string{
		"RestaurantId":   restId,
		"RestaurantName": rdData.RestaurantName,
		"City":           rdData.City,
		"Cuisines":       rdData.Cuisines,
		"RatingsText":    rdData.RatingsText,
		"Votes":          restVotes,
	}
	//Fill write request data into a map.
	fillMap(rdReqMp, dro.rq.rncui)
	strdata := &foodpalaceRqOp{Status: 0}
	strdata.fillRo(dro)
}

//Method to display output of read multi operation and fill the structure.
func (drm *readMulti) displayAndFill(rmdt *foodpalaceRqOp, rdData *foodpalaceapplib.FoodpalaceData, i int) {

	restId := strconv.Itoa(int(rdData.RestaurantId))
	restVotes := strconv.Itoa(int(rdData.Votes))
	log.Info("Result of the read request is:", rdData)
	rdReqMp := map[string]string{
		"RestaurantId":   restId,
		"RestaurantName": rdData.RestaurantName,
		"City":           rdData.City,
		"Cuisines":       rdData.Cuisines,
		"Ratings_text":   rdData.RatingsText,
		"Votes":          restVotes,
	}
	//Fill write request data into a map.
	fillMap(rdReqMp, drm.rmRncui[i])
	rmdt.Status = 0
	rmdt.fillRm(drm)
}

//Method to fill the output of write one operation.
func (wo *foodpalaceRqOp) fillWo(wone *writeOne) {

	//Get timestamp.
	timestamp := getCurrentTime()
	wo.Operation = wone.args[0]
	wo.Timestamp = timestamp
	wo.Data = data
	status := strconv.Itoa(int(wo.Status))
	wrMp := map[string]string{
		"Key":    wone.args[2],
		"Status": status,
	}
	//Fill write request data into a map.
	fillMap(wrMp, wone.args[1])
	//Dump structure into json.
	tmpOutfname := wo.dumpIntoJson(wone.rq.outfileUuid)
	wone.rq.outfilename = tmpOutfname
}

//Method to fill the output of write multi operation.
func (wml *foodpalaceRqOp) fillWm(wmul *writeMulti) {

	//Get timestamp.
	timestamp := getCurrentTime()
	wml.Operation = wmul.rq.operation
	wml.Timestamp = timestamp
	wml.Data = data
	status := strconv.Itoa(int(wml.Status))
	wrMp := map[string]string{
		"Key":    wmul.rq.key,
		"Status": status,
	}
	//Fill write request data into a map.
	fillMap(wrMp, wmul.rq.rncui)
}

//Method to fill the output of read one operation.
func (rof *foodpalaceRqOp) fillRo(rone *readOne) {

	//Get timestamp.
	timestamp := getCurrentTime()
	rof.Operation = rone.rq.operation
	rof.Timestamp = timestamp
	rof.Data = data
	//Dump structure into json.
	tmpoutFilename := rof.dumpIntoJson(rone.rq.outfileUuid)
	rone.rq.outfilename = tmpoutFilename
}

//Method to fill the output of read multi operation.
func (rmf *foodpalaceRqOp) fillRm(fprm *readMulti) {
	//Get timestamp.
	timestamp := getCurrentTime()
	rmf.Operation = fprm.rq.operation
	rmf.Timestamp = timestamp
	rmf.Data = data
}

//prepare method for write one operation to fill the required structure.
func (w *writeOne) prepare() error {

	var err error
	rncui := w.args[1]
	//Typecast RestaurantId to int64.
	restIdStr := w.args[2]
	restId, err := strconv.ParseInt(restIdStr, 10, 64)
	if err != nil {
		log.Error("Error occured in typecasting RestaurantId to int64")
	}

	//Typecast Votes to int64.
	votesStr, err := strconv.ParseInt(w.args[7], 10, 64)
	if err != nil {
		log.Error("Error occured in typecasting Votes to int64")
	}
	//Create a file for storing keys and rncui.
	os.Create("keyRncuiData.txt")
	//Fill the Foodpalace App structure.
	cmddata := foodpalaceapplib.FoodpalaceData{
		RestaurantId:   restId,
		RestaurantName: w.args[3],
		City:           w.args[4],
		Cuisines:       w.args[5],
		RatingsText:    w.args[6],
		Votes:          votesStr,
	}
	w.rq.foodpalaceData = &cmddata
	//Open file for storing key, rncui in append mode.
	file, err := os.OpenFile("keyRncuiData.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Error(err)
	}
	defer file.Close()
	//Append key_rncui in file.
	_, errWr := file.WriteString("key, rncui = " + restIdStr + "  " + rncui + "\n")
	if errWr != nil {
		log.Error(errWr)
	}
	if w.rq.foodpalaceData == nil {
		err = errors.New("prepare method for WriteOne failed")
	}
	return err
}

//exec method for write one operation which performs write operation.
func (woexc *writeOne) exec() error {

	var errorMsg error
	var wrStrdtCmd foodpalaceRqOp
	//Perform write operation.
	err := woexc.rq.clientObj.Write(woexc.rq.foodpalaceData, woexc.args[1])
	if err != nil {
		log.Error("Write key-value failed : ", err)
		wrStrdtCmd.Status = -1
		errorMsg = errors.New("exec method for WriteOne Operation failed.")
	} else {
		log.Info("Pmdb Write successful!")
		wrStrdtCmd.Status = 0
		errorMsg = nil
	}
	wrStrdtCmd.fillWo(woexc)
	return errorMsg
}

//complete method for write one operation which creates final json outfile.
func (woc *writeOne) complete() error {

	var cerr error
	//Copy contents in json outfile.
	err := copyToOutfile(woc.rq.outfilename, woc.args[8])
	if err != nil {
		cerr = errors.New("complete method for WriteOne Operation failed")
	}
	return cerr
}

//prepare method for read one operation to fill the required structure.
func (rop *readOne) prepare() error {

	var err error
	//Typecast key into int64.
	keyInt64, _ := strconv.ParseInt(rop.rq.key, 10, 64)
	//Fill the Foodpalace App structure.
	rdinfo := foodpalaceapplib.FoodpalaceData{
		RestaurantId: keyInt64,
	}
	rop.rq.foodpalaceData = &rdinfo
	if rop.rq.foodpalaceData == nil {
		err = errors.New("prepare method for ReadOne Operation failed")
	} else {
		err = nil
	}
	return err

}

//exec method for read one operation to perform read operation.
func (roe *readOne) exec() error {

	var roerr error
	//Perform read operation.
	rop := &foodpalaceapplib.FoodpalaceData{}
	err := roe.rq.clientObj.Read(roe.rq.foodpalaceData, roe.rq.rncui, rop)
	if err != nil {
		log.Error("Read request failed !!", err)
		rdt := &foodpalaceRqOp{Status: -1}
		rdt.fillRo(roe)
		roerr = errors.New("exec method for ReadOne Operation failed")
	}
	roe.displayAndFill(rop)
	return roerr
}

//complete method for read one operation which creates final json outfile.
func (roc *readOne) complete() error {

	var cerr error
	//Copy contents in json outfile.
	err := copyToOutfile(roc.rq.outfilename, roc.rq.jsonFname)
	if err != nil {
		cerr = errors.New("complete method for ReadOne Operation failed")
	}
	return cerr
}

//prepare method for write multi operation to fill the required structures.
func (wmp *writeMulti) prepare() error {

	var wmperr error
	//Get array of foodpalace_data structure.
	mreqdata := wmp.getWmInfo()
	wmp.multiReqdata = mreqdata
	if wmp.multiReqdata == nil {
		wmperr = errors.New("prepare method for WriteMulti Operation failed")
	} else {
		wmperr = nil
	}
	return wmperr
}

//exec method for write multi operation which performs write operation.
func (wme *writeMulti) exec() error {

	var excerr error
	var wrStrdata = &foodpalaceRqOp{}
	//Create a file for storing keys and rncui.
	os.Create("keyRncuiData.txt")

	//Open file for storing key, rncui in append mode.
	file, err := os.OpenFile("keyRncuiData.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	for i := 0; i < len(wme.multiReqdata); i++ {
		//Generate app_uuid.
		appUuid := uuid.NewV4().String()
		//Create rncui string.
		rncui := appUuid + ":0:0:0:0"
		//Append key_rncui in file.
		restIdStr := strconv.Itoa(int(wme.multiReqdata[i].RestaurantId))
		_, errWrite := file.WriteString("key, rncui = " + restIdStr + "  " + rncui + "\n")
		if errWrite != nil {
			log.Fatal(err)
		}
		wme.rq.key = restIdStr
		wme.rq.rncui = rncui
		err := wme.rq.clientObj.Write(wme.multiReqdata[i], rncui)
		if err != nil {
			log.Error("Pmdb Write failed.", err)
			wrStrdata.Status = -1
			excerr = errors.New("exec method for WriteMulti Operation failed")
		} else {
			log.Info("Pmdb Write successful!")
			wrStrdata.Status = 0
			excerr = nil
		}
		wrStrdata.fillWm(wme)
	}
	//Dump structure into json.
	tmpOutfname := wrStrdata.dumpIntoJson(wme.rq.outfileUuid)
	wme.rq.outfilename = tmpOutfname
	return excerr
}

//complete method for write multi operation which creates final json outfile.
func (wmc *writeMulti) complete() error {
	var cerr error
	//Copy contents in json outfile.
	err := copyToOutfile(wmc.rq.outfilename, wmc.rq.jsonFname)
	if err != nil {
		cerr = errors.New("complete method for WriteMulti Operation failed")
	}
	return cerr
}

///prepare method for read multi operation which creates required structure.
func (rmp *readMulti) prepare() error {
	var prerr error
	var rmreqDt []*foodpalaceapplib.FoodpalaceData
	var rmrncui []string
	f, err := os.Open("keyRncuiData.txt")
	if err != nil {
		log.Error(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		rdallData := strings.Split(scanner.Text(), " ")
		rdallKey := rdallData[3]
		rdallRncui := rdallData[5]
		//Typecast key into int64.
		keyInt64, _ := strconv.ParseInt(rdallKey, 10, 64)
		//Fill the Foodpalace App structure.
		rdDt := foodpalaceapplib.FoodpalaceData{
			RestaurantId: keyInt64,
		}
		rmrncui = append(rmrncui, rdallRncui)
		rmreqDt = append(rmreqDt, &rdDt)
		rmp.rmRncui = rmrncui
		rmp.rmData = rmreqDt
	}
	if rmp.rmData == nil && rmp.rmRncui == nil {
		prerr = errors.New("prepare method for ReadMulti Operation failed")
	} else {
		prerr = nil
	}
	return prerr
}

//exec method for read multi operation which performs read operation.
func (rme *readMulti) exec() error {
	var rmexcerr error
	var rmdte = &foodpalaceRqOp{}
	if len(rme.rmData) == len(rme.rmRncui) {
		for i := range rme.rmData {
			//Perform read operation.
			rmopDt := &foodpalaceapplib.FoodpalaceData{}
			err := rme.rq.clientObj.Read(rme.rmData[i], rme.rmRncui[i], rmopDt)
			if err != nil {
				rmdte = &foodpalaceRqOp{Status: -1}
				rmdte.fillRm(rme)
				rmexcerr = errors.New("exec method for ReadOne Operation failed")
			} else {
				rme.displayAndFill(rmdte, rmopDt, i)
			}
		}
	}
	//Dump structure into json.
	tmpOutfname := rmdte.dumpIntoJson(rme.rq.outfileUuid)
	rme.rq.outfilename = tmpOutfname
	return rmexcerr
}

//complete method for read multi operation which creates final json outfile.
func (rmc *readMulti) complete() error {
	var cerr error
	//Copy contents in json outfile.
	err := copyToOutfile(rmc.rq.outfilename, rmc.rq.jsonFname)
	if err != nil {
		cerr = errors.New("complete method for ReadMulti Operation failed")
	}
	return cerr
}

//prepare method for get leader opeation which creates structure.
func (getLeader *getLeader) prepare() error {

	var gleaerr error
	pmdbItems := &PumiceDBCommon.PMDBInfo{
		RaftUUID:   raftUuid,
		ClientUUID: clientUuid,
	}
	getLeader.pmdbInfo = pmdbItems
	if getLeader.pmdbInfo == nil {
		gleaerr = errors.New("prepare method for get leader operation failed")
	} else {
		gleaerr = nil
	}
	return gleaerr
}

//exec method for get leader operation.
func (getLeader *getLeader) exec() error {

	var glexcerr error
	leaUuid, err := getLeader.rq.clientObj.PmdbGetLeader()
	if err != nil {
		getLeader.rq.status = -1
		log.Error("Failed to get Leader UUID")
	} else {
		getLeader.rq.status = 0
	}
	leaderUuid := leaUuid.String()
	getLeader.pmdbInfo.LeaderUUID = leaderUuid
	if getLeader.pmdbInfo.LeaderUUID == "" {
		glexcerr = errors.New("exec method for get leader operation failed")
	} else {
		log.Error("Leader uuid is:", getLeader.pmdbInfo.LeaderUUID)
		glexcerr = nil
	}
	return glexcerr
}

//complete method for get leader operation which creates final json outfile.
func (getLeader *getLeader) complete() error {

	var cerr error
	//prepare json output filepath.
	jsonOutf := jsonOutFpath + "/" + getLeader.rq.jsonFname + ".json"
	file, err := json.MarshalIndent(getLeader.pmdbInfo, "", "\t")
	err = ioutil.WriteFile(jsonOutf, file, 0644)
	if err != nil {
		cerr = errors.New("complete method for get leader operation failed")
	} else {
		cerr = nil
	}
	return cerr
}

//Function to fill data into map.
func fillMap(mp map[string]string, rncui string) {
	//Fill data into outer map.
	data[rncui] = mp
}

//Method to dump foodpalace app output structure into json file.
func (zj *foodpalaceRqOp) dumpIntoJson(outfUuid string) string {

	//prepare path for temporary json file.
	tmpFname := jsonOutFpath + "/" + outfUuid + ".json"
	file, _ := json.MarshalIndent(zj, "", "\t")
	_ = ioutil.WriteFile(tmpFname, file, 0644)
	return tmpFname
}

//Method to copy temporary json file into output json file.
func copyToOutfile(tmpFname, jsonFname string) error {

	var errcp error
	//prepare json output filepath.
	jsonOutf := jsonOutFpath + "/" + jsonFname + ".json"

	//Create output json file.
	os.Create(jsonOutf)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", tmpFname, jsonOutf).Output()
	if err != nil {
		log.Error("%s", err)
		errcp = err
	} else {
		errcp = nil
	}
	//Remove temporary outfile after copying into json outfile.
	os.Remove(tmpFname)
	return errcp
}

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&clientUuid, "u", "NULL", "client uuid")
	flag.StringVar(&jsonOutFpath, "l", "./", "json_outfilepath")

	flag.Parse()
	log.Info("Raft UUID: ", raftUuid)
	log.Info("Client UUID: ", clientUuid)
	log.Info("Outfile path: ", jsonOutFpath)
}

//Creates log directory if it doesn't exist.
//and if dir path is not passed then it will create log file in current directory by default.
func makeDirectoryIfNotExists() error {
	if _, err := os.Stat(jsonOutFpath); os.IsNotExist(err) {
		return os.Mkdir(jsonOutFpath, os.ModeDir|0755)
	}
	return nil
}

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "--help" || os.Args[1] == "-h" {
		fmt.Println("\nUsage: \n   For help:             ./foodpalaceappclient [-h] \n   To start client:      ./foodpalaceappclient -r [raftUuid] -u [clientUuid] -l [jsonOutfilepath]")
		fmt.Println("\nPositional Arguments: \n   -r    raftUuid \n   -u    clientUuid \n   -l    jsonOutfilepath")
		fmt.Println("\nOptional Arguments: \n   -h, --help            show this help message and exit")
		os.Exit(0)
	}

	//Accept raft and client uuid from cmdline.
	getCmdParams()

	//Create log directory if not exists.
	makeDirectoryIfNotExists()

	//Initialize logger.
	initLogger()

	//Create new client object.
	clientObj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
	if clientObj == nil {
		return
	}
	log.Info("Starting client: ", clientUuid)
	//Start the client.
	clientObj.Start()
	defer clientObj.Stop()

	fmt.Print("\n**********Format for performing operations**********")
	fmt.Print("\nFor WriteOne Operation   => WriteOne#rncui#restaurant id#restaurant name#city#cuisines#ratings text#votes#outfilename")
	fmt.Print("\nFor ReadOne Operation    => ReadOne#key#rncui#outfilename")
	fmt.Print("\nFor WriteMulti Operation => WriteMulti#filename(.csv)#outfilename")
	fmt.Print("\nFor ReadMulti Operation  => ReadMulti#outfilename")
	fmt.Print("\nFor Get Leader Operation => GetLeader#outfilename")

	for {
		fmt.Print("\nEnter operation (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ GetLeader /exit): ")
		input := bufio.NewReader(os.Stdin)
		cmd, _ := input.ReadString('\n')
		cmdS := strings.Replace(cmd, "\n", "", -1)
		opsSplit := strings.Split(cmdS, "#")
		ops := opsSplit[0]

		//Make the required maps.
		data = make(map[string]map[string]string)

		//Generate uuid for temporary json file.
		outfUuid := uuid.NewV4().String()

		//Declare interface variable.
		var fpci FoodpalaceCli

		switch ops {
		case "WriteOne":
			fpci = &writeOne{
				args: opsSplit,
				rq: &rqInfo{
					outfileUuid: outfUuid,
					clientObj:   clientObj,
				},
			}
		case "ReadOne":
			fpci = &readOne{
				rq: &rqInfo{
					key:         opsSplit[1],
					rncui:       opsSplit[2],
					jsonFname:   opsSplit[3],
					operation:   ops,
					outfileUuid: outfUuid,
					clientObj:   clientObj,
				},
			}
		case "WriteMulti":
			fpci = &writeMulti{
				csvFpath: opsSplit[1],
				rq: &rqInfo{
					operation:   ops,
					jsonFname:   opsSplit[2],
					outfileUuid: outfUuid,
					clientObj:   clientObj,
				},
			}
		case "ReadMulti":
			fpci = &readMulti{
				rq: &rqInfo{
					operation:   ops,
					jsonFname:   opsSplit[1],
					outfileUuid: outfUuid,
					clientObj:   clientObj,
				},
			}
		case "GetLeader":
			fpci = &getLeader{
				rq: &rqInfo{
					operation: ops,
					jsonFname: opsSplit[1],
					clientObj: clientObj,
				},
			}
		case "exit":
			os.Exit(0)
		default:
			fmt.Println("Enter valid operation: (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader /exit)")
			continue
		}

		//Perform Operations.
		prerr := fpci.prepare()
		if prerr != nil {
			log.Error(prerr)
		}

		excerr := fpci.exec()
		if excerr != nil {
			log.Error(excerr)
		}

		cerr := fpci.complete()
		if cerr != nil {
			log.Error(cerr)
		}
	}

}
