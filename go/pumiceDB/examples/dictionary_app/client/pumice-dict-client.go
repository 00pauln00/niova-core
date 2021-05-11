package main

import (
	"bufio"
	"flag"
	"fmt"
	"niova/go-pumicedb-lib/client"
	"dictapplib/lib"
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
				leader_uuid := pmdb.GetLeader()
				fmt.Println("Leader uuid is ", leader_uuid)
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

				if ops == "write" {
					//write operation
					pmdb.Write(req_dict, rncui)
				} else {
					/*
					 * Get the size of the structure
					 */
					data_length := pmdb.GetSize(req_dict)
					fmt.Println("Length of the structure: ", data_length)
					/* Retry the read on failure */

					var reply_size int64
					//read operation
					reply_buff := pmdb.Read(req_dict, rncui,
						&reply_size)

					if reply_buff == nil {
						fmt.Println("Read request failed !!")
					} else {
						result_dict := &DictAppLib.Dict_app{}
						pmdb.Decode(reply_buff, result_dict, reply_size)

						fmt.Println("Result of the read request is:")
						fmt.Println("Word: ", input_text)
						fmt.Println("Frequecy of the word: ", result_dict.Dict_wcount)
					}

					// Application should free the this reply_buff which is allocate by pmdb lib
					C.free(reply_buff)
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
