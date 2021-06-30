package httpclient

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"
	"niovakv/niovakvlib"

	log "github.com/sirupsen/logrus"
)

func WriteRequest(reqobj *niovakvlib.NiovaKV, addr, port string) error {
	fmt.Println("Performing Http PUT Request...")
	var Data bytes.Buffer 
	enc := gob.NewEncoder(&Data)

	err := enc.Encode(reqobj)
	if err != nil {
		log.Error(err)
		return err
	}

	connString := "http://" + addr + ":" + port
	req, err := http.NewRequest(http.MethodPut, connString, bytes.NewBuffer(Data.Bytes()))
	if err != nil {
		log.Error(err)
		return err
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Error(err)
		return err
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	//Unmarshal the response.
	responseObj := niovakvlib.NiovaKVResponse{}
	// Convert response body to reqobj struct
	dec := gob.NewDecoder(bytes.NewBuffer(bodyBytes))
	err = dec.Decode(&responseObj)
	if err == nil {
		if reqobj.InputOps == "write" {
			log.Info("Response after successful operation:  ", reqobj.InputOps)
			log.Info("Status of write operation :", responseObj.RespStatus)
		} else {
			log.Info("Response after successful operation:  ", reqobj.InputOps)
			log.Info("Value returned :", string(responseObj.RespValue))
		}
	}
	return err
}

func ReadRequest(reqobj *niovakvlib.NiovaKV, addr, port string) error {
	fmt.Println("Performing Http GET Request...")
	var request bytes.Buffer
	enc := gob.NewEncoder(&request)

	err := enc.Encode(reqobj)
	if err != nil {
		log.Error(err)
		return err
	}

	connString := "http://" + addr + ":" + port
	req, err := http.NewRequest(http.MethodGet, connString, bytes.NewBuffer(request.Bytes()))
	if err != nil {
		log.Error(err)
		return err
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Error(err)
		return err
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	//Unmarshal the response.
	responseObj := niovakvlib.NiovaKVResponse{}
	// Convert response body to reqobj struct
	dec := gob.NewDecoder(bytes.NewBuffer(bodyBytes))
	err = dec.Decode(&responseObj)
	if err == nil {
		if reqobj.InputOps == "write" {
			log.Info("Response after successful operation:  ", reqobj.InputOps)
			log.Info("Status of write operation :", responseObj.RespStatus)
		} else {
			log.Info("Response after successful operation:  ", reqobj.InputOps)
			log.Info("Value returned :", string(responseObj.RespValue))
		}
	}
	return err
}
