package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/http"

	"github.com/jrsmroz/iorate"
)

const loremIpsum = `Lorem ipsum dolor sit amet, 
consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et 
dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation 
ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor 
in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. 
Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia 
deserunt mollit anim id est laborum.`

func loremIpsumHandler(w http.ResponseWriter, req *http.Request) {
	log.Println(req)

	buffered := bufio.NewWriterSize(w, 4*1024*1024)
	var err error
	for err == nil {
		_, err = io.WriteString(buffered, loremIpsum)
	}
	log.Println("request err = ", err)
}

func main() {

	log.Println("Read me at http://localhost:9991/lorem-ipsum -O /dev/null")

	http.HandleFunc("/lorem-ipsum", loremIpsumHandler)

	listener, err := net.Listen("tcp", "localhost:9991")
	if err != nil {
		panic(err)
	}

	limitedListener := iorate.NewLimitedListener(listener)

	listenerLimit := 1 * iorate.MBps
	connLimit := 250 * iorate.KBps
	limitedListener.SetLimits(listenerLimit, connLimit)

	log.Println("listener limit:", listenerLimit)
	log.Println("conn limit:", connLimit)

	http.Serve(limitedListener, nil)
}
