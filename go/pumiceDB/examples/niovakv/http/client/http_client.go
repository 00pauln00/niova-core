package httpclient

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"net/http"
	"niovakv/niovakvlib"

	log "github.com/sirupsen/logrus"
)

func PutRequest(reqobj *niovakvlib.NiovaKV, addr, port string) (*niovakvlib.NiovaKVResponse, error) {
	var Data bytes.Buffer
	responseObj := niovakvlib.NiovaKVResponse{}
	enc := gob.NewEncoder(&Data)
	err := enc.Encode(reqobj)
	if err != nil {
		log.Error(err)
		return &responseObj, err
	}
	connString := "http://" + addr + ":" + port
	req, err := http.NewRequest(http.MethodPut, connString, bytes.NewBuffer(Data.Bytes()))
	if err != nil {
		log.Error(err)
		return &responseObj, err
	}
	log.Info("Write request sent for key : ", reqobj.InputKey)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Error(err)
		return &responseObj, err
	}
	/*
		Check if response if ok,
		If not, fill the response obj with desired values
		If so, fill the resp obj with received values
	*/
	log.Info("HTTP response status : ", resp.Status)
	if resp.StatusCode != 503 {
		//Serviced
		defer resp.Body.Close()
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		//Unmarshal the response.
		dec := gob.NewDecoder(bytes.NewBuffer(bodyBytes))
		err = dec.Decode(&responseObj)
		if err == nil {
			log.Info("Status of write operation for key :", reqobj.InputKey, " ", responseObj.RespStatus)
		}
	} else {
		//Service not found
		responseObj.RespStatus = -1
		responseObj.RespValue = []byte("Server timed out")
	}
	return &responseObj, err
}

func GetRequest(reqobj *niovakvlib.NiovaKV, addr, port string) (*niovakvlib.NiovaKVResponse, error) {
	var request bytes.Buffer
	responseObj := niovakvlib.NiovaKVResponse{}

	enc := gob.NewEncoder(&request)
	err := enc.Encode(reqobj)
	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	connString := "http://" + addr + ":" + port
	req, err := http.NewRequest(http.MethodGet, connString, bytes.NewBuffer(request.Bytes()))
	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	log.Info("HTTP response status : ", resp.Status)
	if resp.StatusCode != 503 {
		//Serviced
		defer resp.Body.Close()
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		//Unmarshal the response.
		dec := gob.NewDecoder(bytes.NewBuffer(bodyBytes))
		err = dec.Decode(&responseObj)
		if err == nil {
			log.Info("Status of read operation for key :", reqobj.InputKey, " ", responseObj.RespStatus)
		}
		
	} else {
		//Service not found
		responseObj.RespStatus = -1
		responseObj.RespValue = []byte("Server timed out")
	}
	return &responseObj, err
}
