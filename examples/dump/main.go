package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

func dump(r *http.Request) {
	fmt.Println("------------ REQUEST -------------")
	fmt.Printf("Method: %s\n", r.Method)
	fmt.Printf("Address: %s\n", r.URL.String())
	body, _ := ioutil.ReadAll(r.Body)
	fmt.Printf("Body:\n%s\n", body)
	fmt.Println("----------------------------------")
}

var (
	addr string
)

func init() {
	flag.StringVar(&addr, "b", ":8092", "Bind address")
}

func main() {
	flag.Parse()
	writeLock := new(sync.Mutex)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(wr http.ResponseWriter, req *http.Request) {
		writeLock.Lock()
		dump(req)
		writeLock.Unlock()
		fmt.Fprintf(wr, "OK")
	})

	srv := &http.Server{}
	srv.Handler = mux
	srv.Addr = addr
	log.Printf("Listening on %s", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
