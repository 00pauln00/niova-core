package main

import (
	"bufio"
	"dictapplib/lib"
	"flag"
	"fmt"
	"niova/go-pumicedb-lib/client"
	"os"
	"strings"
)

/*
#include <stdlib.h>
*/
import "C"

var raft_uuid_go string
var peer_uuid_go string

/*
 Start the pmdb client.
 Read the request from console and process it.
 User will pass the request in following format.
 write.app_uuid.Text => For write opertion.
 read.app_uuid.Word  => For read operation.
 get_leader  => Get the leader uuid.
*/
func pmdbDictClient() {

	//Create new client object.
	pmdb := PumiceDBClient.PmdbClientNew(raft_uuid_go, peer_uuid_go)
	if pmdb == nil {
		return
	}

	//Start the client
	pmdb.Start()
	defer pmdb.Stop()

	for {
		//Read the input from console
		words := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("Enter request in the format ")
			fmt.Print("[get_leader/write/read].app_uuid.text: ")
			text, _ := words.ReadString('\n')

			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			input := strings.Split(text, ".")

			ops := input[0]

			// If user asked for leader uuid
			if ops == "get_leader" {
				var leader_uuid uuid.UUID
				var err error
				leader_uuid, err = client_obj.PmdbGetLeader()
				if err != nil {
					fmt.Errorf("Failed to get Leader UUID")
					continue
				}
				leader_uuid_str := leader_uuid.String()
				fmt.Println("Leader uuid is ", leader_uuid_str)
			} else {
				// If operation is read or write
				rncui := input[1]
				input_text := input[2]

				//Format is: write.AppUUID.text or read.AppUUID.text
				// Prepare the dictionary structure from values passed by user.
				req_dict := DictAppLib.Dict_app{
					Dict_text:   input_text,
					Dict_wcount: 0,
				}

				fmt.Println("rncui: ", rncui)
				fmt.Println("Operation: ", ops)
				fmt.Println("Input string: ", req_dict.Dict_text)

				var err error
				if ops == "write" {
					//write operation
					err = pmdb.Write(req_dict, rncui)
					if err != nil {
						fmt.Println("Write key-value failed : ", err)
						continue
					}
				} else {
					/*
					 * Get the size of the structure
					 */
					data_length := pmdb.GetSize(req_dict)
					fmt.Println("Length of the structure: ", data_length)
					/* Retry the read on failure */

					//read operation
					result_dict := &DictAppLib.Dict_app{}
					err = pmdb.Read(req_dict, rncui, result_dict)

					if err != nil {
						fmt.Println("Read request failed !!: ", err)
					} else {
						fmt.Println("Result of the read request is:")
						fmt.Println("Word: ", input_text)
						fmt.Println("Frequecy of the word: ", result_dict.Dict_wcount)
					}
				}
			}
		}
	}
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "r", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "u", "NULL", "peer uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
}

func main() {
	//Parse the cmdline parameter
	pmdb_dict_app_getopts()

	//Start pmdbDictionary client.
	pmdbDictClient()
}
