package main
import (
	"fmt"
	"os"
	"bufio"
	"strings"
	"flag"
	"gopmdblib/goPmdbClient"
	"gopmdblib/goPmdbCommon"
	"dictapplib/dict_libs"
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
 app_uuid.Text.write => For write opertion.
 app_uuid.Word.write  => For read operation.
*/
func pmdbDictClient() {

	//Start the client.
	pmdb := PumiceDBClient.PmdbStartClient(raft_uuid_go, peer_uuid_go)

	for {
		//Read the input from console
		words := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("Enter request in the format ")
			fmt.Print("app_uuid.text.write/read: ")
			text, _ := words.ReadString('\n')

			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			input := strings.Split(text, ".")
			rncui := input[0]
			input_text := input[1]
			ops := input[2]

			//Format is: AppUUID.text.write or AppUUID.text.read
			// Prepare the dictionary structure from values passed by user.
			req_dict := DictAppLib.Dict_app{
				Dict_text: input_text,
				Dict_wcount: 0,
			}


			fmt.Println("rncui: ", rncui)
			fmt.Println("Operation: ", ops)
			fmt.Println("Input string: ", req_dict.Dict_text)

			if ops == "write" {
				//write operation
				PumiceDBClient.PmdbClientWrite(req_dict, pmdb, rncui)
			} else {
				/*
				 * Get the actual size of the structure
				 */
				length := PumiceDBCommon.GetStructSize(req_dict) + 10
				fmt.Println("Length of the structure: ", length)

				do {
					var reply_size int64
					// Allocate C memory to store the value of the result.
					value_buf := C.malloc(C.size_t(length))

					//read operation
					rc := PumiceDBClient.PmdbClientRead(req_dict, pmdb, rncui, value_buf,
														int64(length), &reply_size)

					if rc < 0 {
						fmt.Println("Read request failed, error: ", rc)
						if rc == os.E2BIG {
							fmt.Println("Allocate bigger buffer and retry read operation")
							length = reply_size
						}
					} else {
						result_dict := &DictAppLib.Dict_app{}
						PumiceDBCommon.Decode(value_buf, result_dict, reply_size)

						fmt.Println("Result of the read request is:")
						fmt.Println("Word: ", input_text)
						fmt.Println("Frequecy of the word: ", result_dict.Dict_wcount)
					}

					C.free(value_buf)
				} while (rc < 0);
			}
		}
	}
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "raft", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "peer", "NULL", "peer uuid")

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
