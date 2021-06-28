package http_client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"niovakv/niovakvlib"
)

func SendRequest(reqobj *niovakvlib.NiovaKV, addr string, port string) error {

	fmt.Println("Performing Http Request...")
	jsonReq, _ := json.Marshal(reqobj)
	url := "http://" + addr + ":" + port
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsonReq))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
		return err
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	responseObj := niovakvlib.NiovaKVResponse{}
	// Convert response body to response struct
	err = json.Unmarshal(bodyBytes, &responseObj)
	return err
}
