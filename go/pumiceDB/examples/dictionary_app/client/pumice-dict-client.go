package main

import (
	"bufio"
	"dictapplib/lib"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"

	"niova/go-pumicedb-lib/client"
)

/*
#include <stdlib.h>
*/
import "C"

var raft_uuid_go string
var peer_uuid_go string
var async_ops = false

func main() {
	//Parse the cmdline parameter
	pmdb_dict_app_getopts()

	//Start pmdbDictionary client.
	pmdbDictClient()
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "r", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "u", "NULL", "peer uuid")
	async_ptr := flag.Bool("a", false, "Async operation")
	async_ops = *async_ptr
	// Check if async option is passed.

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
	fmt.Println("Async write/read: ", async_ops)
}

/*
 Start the pmdb client.
 Read the request from console and process it.
 User will pass the request in following format.
 write.app_uuid.Text => For write opertion.
 read.app_uuid.Word  => For read operation.
 get_leader  => Get the leader uuid.
*/
func pmdbDictClient() {

	var err error
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

			switch ops {
			case "get_leader":
				var leader_uuid uuid.UUID
				leader_uuid, err = pmdb.PmdbGetLeader()
				if err != nil {
					fmt.Errorf("Failed to get Leader UUID")
					continue
				}
				leader_uuid_str := leader_uuid.String()
				fmt.Println("Leader uuid is ", leader_uuid_str)

			case "write":
				//Prepare write request
				wr_req_dict := DictAppLib.Dict_app{
					Dict_text:   input[2],
					Dict_wcount: 0,
				}

				err = pmdb.Write(wr_req_dict, input[1], async_ops)
				if err != nil {
					fmt.Println("Write key-value failed : ", err)
					continue
				}
			case "read":
				//Prepare read request
				rd_req_dict := DictAppLib.Dict_app{
					Dict_text:   input[2],
					Dict_wcount: 0,
				}

				rd_op_dict := &DictAppLib.Dict_app{}
				err = pmdb.Read(rd_req_dict, input[1], rd_op_dict, async_ops)

				if err != nil {
					fmt.Println("Read request failed !!: ", err)
				} else {
					fmt.Println("Result of the read request is:")
					fmt.Println("Word: ", rd_op_dict.Dict_text)
					fmt.Println("Frequecy of the word: ", rd_op_dict.Dict_wcount)
				}
			}
		}
	}
}
