package main

import (
 "os"
 "fmt"
 "io/ioutil"
 "common/requestResponseLib"
 "encoding/json"
 "encoding/gob"
 "errors"
 "flag"
 uuid "github.com/satori/go.uuid"
 pmdbClient "niova/go-pumicedb-lib/client"
 "bytes"
)


type configApplication struct {
	pmdbClientObj *pmdbClient.PmdbClientObj
	raftUUID string
	clientUUID string
	configFile string
}

func (handler *configApplication) getCmdLineArgs() {
	flag.StringVar(&handler.raftUUID, "r", "NULL", "Raft UUID")
	flag.StringVar(&handler.configFile, "c", "NULL", "Config file path")
}

func (handler *configApplication) startPMDBClient() error {
        var err error

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

        //Store rncui in clientObj
        handler.pmdbClientObj.AppUUID = uuid.NewV4().String()
        return nil

}

func (handler *configApplication) Write(key string, data []byte) error{
	var request requestResponseLib.KVRequest
	request.Operation = "write"
	request.Key = key
	request.Value = data
	request.Rncui = uuid.NewV4().String()+"0:0:0:0"
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	enc.Encode(request)
	err := handler.pmdbClientObj.WriteEncoded(requestBytes.Bytes(), request.Rncui)
	return err
}


/*
func (handler *configApplication) ReadCallBack(request []byte, response *[]byte) error {
        return handler.pmdbClientObj.ReadEncoded(request, "", response)
}
*/

func main() {
	appHandler := configApplication{
		clientUUID:uuid.NewV4().String(),
	}
	appHandler.getCmdLineArgs()
	appHandler.startPMDBClient()

	//get config from json file
	jsonFile, err := os.Open(appHandler.configFile)
	if err != nil {
    		fmt.Println(err)
	}

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
                fmt.Println(err)
        }

	var cfgData map[string]interface{}
    	json.Unmarshal([]byte(byteValue), &cfgData)


	for key,value := range cfgData {
		data, _ := json.Marshal(value)
		appHandler.Write(key,data)
	}
	
}
