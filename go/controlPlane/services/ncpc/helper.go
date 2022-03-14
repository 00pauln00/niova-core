package main

import (
	compressionLib "common/specificCompressionLib"
	"encoding/json"
	"io/ioutil"
	"fmt"
)

//Write to Json
func (cli *clientHandler) write2Json(toJson map[string][]opData) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
}

func (cli *clientHandler) getNISDInfo() map[string]nisdData {
	data := cli.clientAPIObj.GetMembership()
	nisdDataMap := make(map[string]nisdData)
	for _, node := range data {
		if (node.Tags["Type"] == "LOOKOUT") && (node.Status == "alive") {
			for cuuid, value := range node.Tags {
				uuid, err := compressionLib.DecompressUUID(cuuid)
				if err == nil {
					CompressedStatus := value[0]

					//Decompress
					thisNISDData := nisdData{}
					thisNISDData.UUID = uuid
					fmt.Println("NISD Status : ", CompressedStatus)
					if string(CompressedStatus) == "1" {
						thisNISDData.Status = "Alive"
					} else {
						thisNISDData.Status = "Dead"
					}

					nisdDataMap[uuid] = thisNISDData
				}
			}
		}
	}
	return nisdDataMap
}
