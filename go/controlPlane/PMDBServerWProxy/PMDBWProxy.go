package main

import (
	PMDBServer "controlplane/PMDBServer"
	Proxy "controlplane/Proxy"
	"fmt"
	"os"
)

func main() {
	fmt.Println("PMDBWProxy started")

	pmdbServerArgs := []string{}
	proxyArgs := []string{}

	// Start after program name
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		// Simple splitting logic: you could be more advanced here if needed
		if arg == "--SERVER" {
			// all following args are for app1
			i++
			for ; i < len(os.Args) && os.Args[i] != "--PROXY"; i++ {
				pmdbServerArgs = append(pmdbServerArgs, os.Args[i])
			}
			i-- // step back
		} else if arg == "--PROXY" {
			i++
			for ; i < len(os.Args) && os.Args[i] != "--SERVER"; i++ {
				proxyArgs = append(proxyArgs, os.Args[i])
			}
			i--
		}
	}

	if len(pmdbServerArgs) != 0 {
		fmt.Println("PMDBServer Args:", pmdbServerArgs)
		go PMDBServer.Run(pmdbServerArgs)
	}
	if len(proxyArgs) != 0 {
		fmt.Println("Proxy Args:", proxyArgs)
		go Proxy.Run(proxyArgs)
	}

	select {}
}
