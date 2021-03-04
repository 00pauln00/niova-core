package main
import (
	"fmt"
	"os"
	"unsafe"
	"bufio"
	"strings"
	"flag"
	"gopmdblib/goPmdb"
	"dictapplib/dict_libs"
)

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
	pmdb := GoPmdb.PmdbStartClient(raft_uuid_go, peer_uuid_go)

	for {
		//Read the input from console
		words := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("Enter request in the format ")
			fmt.Print("app_uuid.text.write/read")
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


			fmt.Println("rncui", rncui)
			fmt.Println("Operation", ops)
			fmt.Println("text", req_dict.Dict_text)

			if ops == "write" {
				//write operation
				GoPmdb.PmdbClientWrite(req_dict, pmdb, rncui)
			} else {
				/*
				 * Get the value len, would be same as req_dict
				 * Encode function will convert the struct to byte array
				 * and will return total bytes taken by structure.
				 */
				var length int64
				GoPmdb.Encode(req_dict, &length)
				fmt.Println("Length of the structure: ", length)

				//read operation
				value_buf := GoPmdb.PmdbClientRead(req_dict, pmdb, rncui, int64(value_len))

				fmt.Println("Decode the result")
				//Decode the output into Dict_app
				result_dict := &DictAppLib.Dict_app{}
				GoPmdb.Decode(unsafe.Pointer(value_buf), result_dict, length)

				fmt.Println("Word: ", result_dict.Dict_text)
				fmt.Println("Frequecy of the word: ", result_dict.Dict_wcount)
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
