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

func SendRequest(reqobj *niovakvlib.NiovaKV) {

	fmt.Println("Performing Http Request...")
	jsonReq, err := json.Marshal(reqobj)
	req, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:8000", bytes.NewBuffer(jsonReq))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	responseObj := &niovakvlib.NiovaKVResponse{}
	// Convert response body to Todo struct
	data := json.Unmarshal(bodyBytes, responseObj)
	fmt.Println("Data:", data)
	// Print the ouput in JSON
}
