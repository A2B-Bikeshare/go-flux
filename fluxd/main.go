package main

import (
	"bytes"
	"github.com/bitly/go-nsq"
	"github.com/philhofer/fluxlog"
	"net/http"
	"runtime"
	"sync"
)

var (
	lookupdaddr = "http://localhost:9200"
	bpl         *bufPool
)

func init() {
	bpl = &bufPool{new(sync.Pool)}
}

func main() {
	ncpu := runtime.NumCPU()
	runtime.GOMAXPROCS(ncpu)

}
