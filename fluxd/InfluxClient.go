package main

import (
	"bytes"
	"fmt"
	"github.com/philhofer/fluxlog"
	"io"
	"net/http"
	"strconv"
)

var (
	INFLUXPREFIX []byte = strconv.AppendQuoteRune([]byte{}, '[') //Data Prefix
	INFLUXSUFFIX []byte = strconv.AppendQuoteRune([]byte{}, ']') //Data Suffix
)

type InfluxConn struct {
	pwd   string
	usr   string
	tprec string
}

func (i *InfluxConn) Request(saddr string, topic string, b *bytes.Buffer) (*http.Request, error) {
	url := fmt.Sprintf("%s/db/%s/series?u=%s&p=%s", saddr, topic, i.usr, i.pwd)

	return http.NewRequest("POST", url, b)
}

func (i *InfluxConn) Prefix() []byte {
	return INFLUXPREFIX
}

func (i *InfluxConn) Suffix() []byte {
	return INFLUXSUFFIX
}

func (i *InfluxConn) Decode(s fluxlog.CapEntry, w io.Writer) error {
	return fluxlog.InfluxDBDecode(s, w)
}
