package main

import (
	"bytes"
	"fmt"
	"github.com/philhofer/fluxlog"
	"io"
	"net/http"
	"strconv"
)

const ESDATATYPE = "entry"

var (
	ESPREFIX []byte = strconv.AppendQuote([]byte{}, `{"create":{"_type":"entry"}}`)
	ESSUFFIX []byte = strconv.AppendQuote([]byte{}, `}\n`)
)

type ESConn struct{}

func (e *ESConn) Request(saddr string, topic string, b *bytes.Buffer) (req *http.Request, err error) {
	return http.NewRequest("POST", fmt.Sprintf("%s/%s/%s/", saddr, topic, ESDATATYPE), b)
}

func (e *ESConn) Validate(res *http.Response) error {
	switch res.StatusCode {
	case 200, 201:
		return nil
	default:
		return fmt.Errorf("Bad Response.")
	}
}

func (e *ESConn) Prefix() []byte {
	return ESPREFIX
}

func (e *ESConn) Suffix() []byte {
	return ESSUFFIX
}

func (e *ESConn) Decode(s fluxlog.CapEntry, w io.Writer) error {
	return fluxlog.ElasticSearchDecode(s, w)
}
