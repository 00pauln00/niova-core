package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"covidapplib/lib"
	"encoding/csv"
	"encoding/json"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"niova/go-pumicedb-lib/client"
	"niova/go-pumicedb-lib/common"
)

var (
	raftUuid      string
	clientUuid    string
	jsonFilePath  string
	rwMap         map[string]map[string]string
	keyRncuiMap   map[string]string
	writeMultiMap map[CovidAppLib.CovidLocale]string
)

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "-h" {
		fmt.Println("Positional Arguments: \n		'-r' - RAFT UUID \n		'-u' - CLIENT UUID")
		fmt.Println("optional Arguments: \n		'-l' - Json and Log File Path \n		-h, -help")
		fmt.Println("covid_app_client -r <RAFT UUID> -u <CLIENT UUID> -l <log directory>")
		os.Exit(0)
	}

	//Parse the cmdline parameter
	parseArgs()

	//Create log directory if not Exist.
	makeDirectoryIfNotExists()

	//Create log file.
	initLogger()

	log.Info("Raft UUID: ", raftUuid)
	log.Info("Client UUID: ", clientUuid)
	log.Info("Outfile Path: ", jsonFilePath)

	//Create new client object.
	clientObj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
	if clientObj == nil {
		return
	}

	//Start the client
	clientObj.Start()
	defer clientObj.Stop()

	fmt.Println("=================Format to pass write-read entries================")
	fmt.Println("Single write format ==> WriteOne#rncui#key#Val0#Val1#Val2#outfile_name")
	fmt.Println("Single read format ==> ReadOne#key#rncui#outfile_name")
	fmt.Println("Multiple write format ==> WriteMulti#csvfile.csv#outfile_name")
	fmt.Println("Multiple read format ==> ReadMulti#outfile_name")
	fmt.Println("Get Leader format ==> GetLeader#outfile_name")

	for {

		fmt.Print("Enter Operation(WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ GetLeader/ exit): ")

		//Get console input string
		var str string

		//Split the inout string.
		input, err := getInput(str)
		fmt.Println("input: ",input)
		fmt.Printf("err: %v\n", err)

		ops := input[0]
		fmt.Println("ops:",ops)

		//Create and Initialize map for write-read oufile.
		rwMap = make(map[string]map[string]string)

		//Create and Initialize the map for WriteMulti
		writeMultiMap = make(map[CovidAppLib.CovidLocale]string)

		///Create temporary UUID
		tempUuid := uuid.New()
		tempUuidStr := tempUuid.String()

		var opIface Operation

		switch ops {
		case "WriteOne":
			opIface = &wrOne{
				op: opInfo{
					outfileUuid:  tempUuidStr,
					jsonFileName: input[6],
					key:          input[2],
					rncui:        input[1],
					inputStr:     input,
					cliObj:       clientObj,
				},
			}
		case "ReadOne":
			opIface = &rdOne{
				op: opInfo{
					outfileUuid:  tempUuidStr,
					jsonFileName: input[3],
					key:          input[1],
					rncui:        input[2],
					inputStr:     input,
					cliObj:       clientObj,
				},
			}
		case "WriteMulti":
			opIface = &wrMul{
				csvFile: input[1],
				op: opInfo{
					outfileUuid:  tempUuidStr,
					jsonFileName: input[2],
					inputStr:     input,
					cliObj:       clientObj,
				},
			}
		case "ReadMulti":
			opIface = &rdMul{
				op: opInfo{
					outfileUuid:  tempUuidStr,
					jsonFileName: input[1],
					inputStr:     input,
					cliObj:       clientObj,
				},
			}
		case "GetLeader":
			opIface = &getLeader{
				op: opInfo{
					jsonFileName: input[1],
					inputStr:     input,
					cliObj:       clientObj,
				},
			}
		default:
			fmt.Println("\nEnter valid Operation: WriteOne/ReadOne/WriteMulti/ReadMulti/GetLeader/exit")
			continue
		}
		prepErr := opIface.prepare()
		if prepErr != nil {
			log.Error("error to call prepare() method")
			os.Exit(0)
		}
		execErr := opIface.exec()
		if execErr != nil {
			log.Error("error to call exec() method")
			os.Exit(0)
		}
		compErr := opIface.complete()
		if compErr != nil {
			log.Error("error to call complete() method")
			os.Exit(0)
		}
	}
}

//Positional Arguments.
func parseArgs() {

	flag.StringVar(&raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&clientUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&jsonFilePath, "l", "/tmp/covidAppLog", "json outfile path")

	flag.Parse()
}

/*If log directory is not exist it creates directory.
  and if dir path is not passed then it will create
  log file in "/tmp/covidAppLog" path.
*/
func makeDirectoryIfNotExists() error {

	if _, err := os.Stat(jsonFilePath); os.IsNotExist(err) {

		return os.Mkdir(jsonFilePath, os.ModeDir|0755)
	}

	return nil
}

//Create logfile for client.
func initLogger() {

	var filename string = jsonFilePath + "/" + clientUuid + ".log"

	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.i
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Timestamp format for logfile.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		log.Error(err)
	} else {
		log.SetOutput(f)
	}
}

//Interface for Operation.
type Operation interface {
	prepare() error  //Fill Structure.
	exec() error     //Write-Read Operation.
	complete() error //Create Output Json File.
}

/*
 Structure for Common items.
*/
type opInfo struct {
	outfileUuid  string
	outfileName  string
	jsonFileName string
	key          string
	rncui        string
	inputStr     []string
	covidData    *CovidAppLib.CovidLocale
	cliObj       *PumiceDBClient.PmdbClientObj
}

/*
 Structure for WriteOne Operation.
*/
type wrOne struct {
	op opInfo
}

/*
 Structure for ReadOne Operation.
*/
type rdOne struct {
	op opInfo
}

/*
 Structure for WriteMulti Operation.
*/
type wrMul struct {
	csvFile string
	op      opInfo
}

/*
 Structure for ReadMulti Operation.
*/
type rdMul struct {
	multiRead []*CovidAppLib.CovidLocale
	rdRncui   []string
	op        opInfo
}

/*
 Structure for GetLeader Operation.
*/
type getLeader struct {
	op       opInfo
	pmdbInfo *PumiceDBCommon.PMDBInfo
}

/*
 Structure to create json outfile.
*/
type covidVaxData struct {
	Operation string
	Status    int
	Timestamp string
	Data      map[string]map[string]string
}

//Function to read-write data into map.
func fillDataToMap(mp map[string]string, rncui string) {

	//Fill rwMap into outer map.
	rwMap[rncui] = mp
}

//Method to dump CovidVaxData structure into json file.
func (cvd *covidVaxData) dumpIntoJson(outfileUuid string) string {

	//prepare path for temporary json file.
	tempOutfileName := jsonFilePath + "/" + outfileUuid + ".json"
	file, _ := json.MarshalIndent(cvd, "", "\t")
	_ = ioutil.WriteFile(tempOutfileName, file, 0644)

	return tempOutfileName

}

//read console input.
func getInput(keyText string) ([]string, error) {

	//Read the key from console
	key := bufio.NewReader(os.Stdin)

	keyText, _ = key.ReadString('\n')
	fmt.Println("\nkeyText:", keyText)

	// convert CRLF to LF
	keyText = strings.Replace(keyText, "\n", "", -1)

	if keyText == "exit" {
		os.Exit(0)
	}

	input := strings.Split(keyText, "#")
	for i := range input {
		input[i] = strings.TrimSpace(input[i])
	}
	fmt.Println("In getInput , input:",input)

	if len(input) == 1 {
		return nil, errors.New("delimiter not found")
	}

	return input, nil
}

/*This function stores rncui for all csv file
  rwMap into a keyRncuiMap and returns that rncui.
*/
func getRncui(keyRncuiMap map[string]string,
	cwr *CovidAppLib.CovidLocale) string {

	appUuid := uuid.New()

	//Generate app_uuid.
	appUuidStr := appUuid.String()

	//Create rncui string.
	rncui := appUuidStr + ":0:0:0:0"

	keyRncuiMap[cwr.Location] = rncui

	return rncui
}

//parse csv file.
func parseCSV(filename string) (fp *csv.Reader) {

	// open the filei
	csvfile, err := os.Open(filename)
	if err != nil {
		log.Error("Error to open the csv file:", err)
	}

	// Skip first row (line)
	row1, err := bufio.NewReader(csvfile).ReadSlice('\n')
	if err != nil {
		log.Error("Error to skip first row from csvfile:", err)
	}
	_, err = csvfile.Seek(int64(len(row1)), io.SeekStart)
	if err != nil {
		log.Error(err)
	}
	// Parse the file
	fp = csv.NewReader(csvfile)

	return fp
}

//Get timestamp to dump into json outfile.
func getCurrentTime() string {

	//Get Timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	return timestamp
}

//Fill the Json data into map for WriteOne Operation.
func (cvd *covidVaxData) fillWriteOne(wrOneObj *wrOne) {

	//Get current time.
	timestamp := getCurrentTime()

	//fill the value into json structure.
	cvd.Operation = wrOneObj.op.inputStr[0]
	cvd.Timestamp = timestamp
	cvd.Data = rwMap

	StatStr := strconv.Itoa(int(cvd.Status))
	writeMp := map[string]string{
		"key":    wrOneObj.op.key,
		"Status": StatStr,
	}

	//fill write request data into a map.
	fillDataToMap(writeMp, wrOneObj.op.rncui)

}

//Fill the Json data into map for ReadOne Operation.
func (cvd *covidVaxData) fillReadOne(rdOneObj *rdOne) {

	//Get current time.
	timestamp := getCurrentTime()

	//Fill the value into Json structure.
	cvd.Operation = rdOneObj.op.inputStr[0]
	cvd.Timestamp = timestamp
	cvd.Data = rwMap
}

//Fill the Json data into map for WriteMulti Operation.
func (cvd *covidVaxData) fillWriteMulti(wm *wrMul) {

	//Get current time.
	timestamp := getCurrentTime()

	//fill the value into json structure.
	cvd.Operation = wm.op.inputStr[0]
	cvd.Timestamp = timestamp
	cvd.Data = rwMap

	StatStr := strconv.Itoa(int(cvd.Status))
	writeMp := map[string]string{
		"key":    wm.op.key,
		"Status": StatStr,
	}

	//fill write request rwMap into a map.
	fillDataToMap(writeMp, wm.op.rncui)
}

//Fill the Json data into map for ReadMulti Operation.
func (cvd *covidVaxData) fillReadMulti(rm *rdMul) {

	//Get current time.
	timestamp := getCurrentTime()

	//Fill the value into Json structure.
	cvd.Operation = rm.op.inputStr[0]
	cvd.Timestamp = timestamp
	cvd.Data = rwMap
}

//Copy temporary outfile into actual Json file.
func copyToJsonFile(tempOutfileName string, jsonFileName string) error {

	var cp_err error
	//prepare json output filepath.
	jsonOut := jsonFilePath + "/" + jsonFileName + ".json"

	//Create output json file.
	os.Create(jsonOut)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", tempOutfileName, jsonOut).Output()

	if err != nil {
		log.Error("Failed to copy data to json file: %s", err)
		cp_err = err
	} else {
		cp_err = nil
	}

	//Remove temporary outfile after copying into json outfile.
	e := os.Remove(tempOutfileName)
	if e != nil {
		log.Error("Failed to remove temporary outfile:%s", e)
	}
	return cp_err
}

//prepare() method to fill structure for WriteOne.
func (wrObj *wrOne) prepare() error {

	var err error

	isoVal := wrObj.op.inputStr[3]
	tvVal := wrObj.op.inputStr[4]
	pvVal := wrObj.op.inputStr[5]

	//typecast the int64 type of csv file.
	tvInt, _ := strconv.ParseInt(tvVal, 10, 64)
	pvInt, _ := strconv.ParseInt(pvVal, 10, 64)

	/*
	   prepare the structure from values passed by user.
	   fill the struture
	*/
	inputStruct := CovidAppLib.CovidLocale{
		Location:          wrObj.op.key,
		IsoCode:           isoVal,
		TotalVaccinations: tvInt,
		PeopleVaccinated:  pvInt,
	}

	wrObj.op.covidData = &inputStruct

	if wrObj.op.covidData == nil {
		err = errors.New("prepare() method failed for WriteOne.")
	}

	return err
}

/*
  exec() method for  WriteOne to write rwMap
  and dump to json file.
*/
func (wrObj *wrOne) exec() error {

	var errMsg error
	var wrData = &covidVaxData{}

	//Perform write Operation.
	err := wrObj.op.cliObj.Write(wrObj.op.covidData,
		wrObj.op.rncui)

	if err != nil {
		errMsg = errors.New("exec() method failed for WriteOne.")
		wrData.Status = -1
		log.Info("Write key-value failed : ", err)
	} else {
		log.Info("Pmdb Write successful!")
		wrData.Status = 0
		errMsg = nil
	}

	wrData.fillWriteOne(wrObj)

	//Dump structure into json.
	wrObj.op.outfileName = wrData.dumpIntoJson(wrObj.op.outfileUuid)

	return errMsg
}

/*
  complete() method for WriteOne to
  create output Json file.
*/
func (wrObj *wrOne) complete() error {

	var cErr error

	//Copy temporary json file into json outfile.
	err := copyToJsonFile(wrObj.op.outfileName,
		wrObj.op.jsonFileName)

	if err != nil {
		cErr = errors.New("complete() method failed for WriteOne.")
	}

	return cErr
}

//prepare() method to fill structure for ReadOne.
func (rdObj *rdOne) prepare() error {

	var err error

	inputStruct := CovidAppLib.CovidLocale{
		Location: rdObj.op.key,
	}

	rdObj.op.covidData = &inputStruct

	if rdObj.op.covidData == nil {
		err = errors.New("prepare() method failed for ReadOne.")
	}

	return err
}

/*
  exec() method for  ReadOne to read rwMap
  and dump to json file.
*/
func (rdObj *rdOne) exec() error {

	var rErr error
	var rdData = &covidVaxData{}

	resStruct := &CovidAppLib.CovidLocale{}

	//read Operation
	err := rdObj.op.cliObj.Read(rdObj.op.covidData, "",
		resStruct)

	if err != nil {

		log.Info("Read request failed !!", err)
		rdData.Status = -1
		rdData.fillReadOne(rdObj)
		rErr = errors.New("exec() method failed for ReadOne")

	} else {

		log.Info("Result of the read request is: ", resStruct)

		//typecast int64 type data of csv file.
		totalVax := strconv.Itoa(int(resStruct.TotalVaccinations))
		peopleVax := strconv.Itoa(int(resStruct.PeopleVaccinated))

		readMap := map[string]string{
			"Location":          resStruct.Location,
			"IsoCode":           resStruct.IsoCode,
			"TotalVaccinations": totalVax,
			"PeopleVaccinated":  peopleVax,
		}

		//Fill write request rwMap into a map.
		fillDataToMap(readMap, rdObj.op.rncui)
		rdData.Status = 0
		rdData.fillReadOne(rdObj)
		rErr = nil
	}

	//Dump structure into json.
	rdObj.op.outfileName = rdData.dumpIntoJson(rdObj.op.outfileUuid)

	return rErr
}

/*
  complete() method for ReadOne to
  create output Json file.
*/
func (rdObj *rdOne) complete() error {

	var cErr error

	//Copy temporary json file into json outfile.
	err := copyToJsonFile(rdObj.op.outfileName,
		rdObj.op.jsonFileName)

	if err != nil {
		cErr = errors.New("complete() method failed for ReadOne.")
	}

	return cErr
}

//prepare() method to fill structure for WriteMulti.
func (wmObj *wrMul) prepare() error {

	var err error

	/*Create and Initialize the map for multiple
	keys and its rncui.*/
	keyRncuiMap = make(map[string]string)

	//call function to parse csv file.
	fp := parseCSV(wmObj.csvFile)

	for {
		// Read each record from csv
		record, err := fp.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error(err)
		}

		//typecast the int64 type data of csv file.
		totalVax, _ := strconv.ParseInt(record[3], 10, 64)
		peopleVax, _ := strconv.ParseInt(record[4], 10, 64)

		//fill the struture
		cwr := &CovidAppLib.CovidLocale{
			Location:          record[0],
			IsoCode:           record[1],
			TotalVaccinations: totalVax,
			PeopleVaccinated:  peopleVax,
		}

		//Fill the map for each structure of csv record.
		writeMultiMap[*cwr] = "record_struct"

		if writeMultiMap == nil {
			err = errors.New("prepare() method failed for WriteMulti Operation.")
		} else {
			err = nil
		}
	}

	return err
}

/*
  exec() method for WriteMulti to write data
  from csv file and dump to json file.
*/
func (wmObj *wrMul) exec() error {

	var wErr error
	var wmData = &covidVaxData{}

	for csvStruct, _ := range writeMultiMap {
		rncui := getRncui(keyRncuiMap, &csvStruct)
		wmObj.op.key = csvStruct.Location
		wmObj.op.rncui = rncui
		err := wmObj.op.cliObj.Write(&csvStruct, rncui)

		if err != nil {
			wmData.Status = -1
			log.Info("Write key-value failed : ", err)
			wErr = errors.New("exec() method failed for WriteMulti Operation.")
		} else {
			log.Info("Pmdb Write successful!")
			wmData.Status = 0
			wErr = nil
		}
		wmData.fillWriteMulti(wmObj)
	}

	//Dump structure into json.
	wmObj.op.outfileName = wmData.dumpIntoJson(wmObj.op.outfileUuid)

	return wErr
}

/*
  complete() method for WriteMulti to
  create output Json file.
*/
func (wmObj *wrMul) complete() error {

	var cErr error

	//Copy temporary json file into json outfile.
	err := copyToJsonFile(wmObj.op.outfileName,
		wmObj.op.jsonFileName)
	if err != nil {
		cErr = errors.New("complete() method failed for ReadOne.")
	}

	return cErr
}

//prepare() method to fill structure for ReadMulti.
func (rmObj *rdMul) prepare() error {

	var err error
	var rmRncui []string
	var rmData []*CovidAppLib.CovidLocale

	for key, rncui := range keyRncuiMap {
		log.Info(key, ":", rncui)
		crd := CovidAppLib.CovidLocale{
			Location: key,
		}
		rmRncui = append(rmRncui, rncui)
		rmObj.rdRncui = rmRncui
		rmData = append(rmData, &crd)
		rmObj.multiRead = rmData

		if rmObj.multiRead == nil && rmObj.rdRncui == nil {
			err = errors.New("prepare() method failed for ReadMulti.")
		} else {
			err = nil
		}
	}

	return err
}

/*exec() method for ReadMulti to read data
  of csv file and dump to json file.*/
func (rmObj *rdMul) exec() error {

	var rErr error
	//var reply_size int64
	var rmData = &covidVaxData{}

	if len(rmObj.multiRead) == len(rmObj.rdRncui) {

		for i := range rmObj.rdRncui {

			resStruct := &CovidAppLib.CovidLocale{}
			err := rmObj.op.cliObj.Read(rmObj.multiRead[i], "", resStruct)

			if err != nil {

				log.Info("Read request failed !!", err)

				rmData.Status = -1
				rmData.fillReadMulti(rmObj)
				rErr = errors.New("exec() method failed for ReadMulti")

			} else {

				log.Info("Result of the read request is: ", resStruct)

				totalVax := strconv.Itoa(int(resStruct.TotalVaccinations))
				peopleVax := strconv.Itoa(int(resStruct.PeopleVaccinated))

				readMap := map[string]string{
					"Location":          resStruct.Location,
					"IsoCode":           resStruct.IsoCode,
					"TotalVaccinations": totalVax,
					"PeopleVaccinated":  peopleVax,
				}

				//Fill write request rwMap into a map.
				fillDataToMap(readMap, rmObj.rdRncui[i])
				rmData.Status = 0
				rmData.fillReadMulti(rmObj)
				rErr = nil
			}
		}
	}

	//Dump structure into json.
	rmObj.op.outfileName = rmData.dumpIntoJson(rmObj.op.outfileUuid)

	return rErr
}

/*
  complete() method for ReadMulti to
  create output Json file.
*/
func (rmObj *rdMul) complete() error {

	var cErr error

	//Copy temporary json file into json outfile.
	err := copyToJsonFile(rmObj.op.outfileName,
		rmObj.op.jsonFileName)
	if err != nil {
		cErr = errors.New("complete() method failed for ReadMulti.")
	}

	return cErr
}

//prepare() method to get leader.
func (getleader *getLeader) prepare() error {

	var err error

	pmdbItems := &PumiceDBCommon.PMDBInfo{
		RaftUUID:   raftUuid,
		ClientUUID: clientUuid,
	}

	getleader.pmdbInfo = pmdbItems

	if getleader.pmdbInfo == nil {
		err = errors.New("prepare() method failed for GetLeader Operation.")
	} else {
		err = nil
	}

	return err
}

//exec() method to get leader.
func (getleader *getLeader) exec() error {

	var err error
	var leaderUuid uuid.UUID

	leaderUuid, err = getleader.op.cliObj.PmdbGetLeader()

	if err != nil {
		return fmt.Errorf("Failed to get Leader UUID")
	}

	leaderUuidStr := leaderUuid.String()
	getleader.pmdbInfo.LeaderUUID = leaderUuidStr

	log.Info("Leader uuid is ", getleader.pmdbInfo.LeaderUUID)

	return err
}

/*
  complete() method for Get Leader to
  create output Json file.
*/
func (getleader *getLeader) complete() error {

	var cerr error

	//prepare path for json file.
	jsonOutfile := jsonFilePath + "/" + getleader.op.jsonFileName + ".json"
	file, cerr := json.MarshalIndent(getleader.pmdbInfo, "", "\t")
	cerr = ioutil.WriteFile(jsonOutfile, file, 0644)

	if cerr != nil {
		return nil
	}

	return ioutil.WriteFile(jsonOutfile, file, 0644)
}
