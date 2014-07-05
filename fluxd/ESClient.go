package fluxd

import (
	"bytes"
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"io"
	"net/http"
	"sync"
)

// ElasticsearchDB conforms to the
// DB interface. It POSTs to http://{Addr}/{Index}/{Dtype}/
// using the output of Translate() as the message body.
type ElasticsearchDB struct {
	Schema msg.Schema
	Addr   string
	Index  string
	Dtype  string
	fqaddr string
	once   *sync.Once
}

// Address returns the endpoint that this db POSTs to.
func (e ElasticsearchDB) Address() string {
	e.once.Do(func() {
		e.fqaddr = fmt.Sprintf("%s/%s/%s", e.Addr, e.Index, e.Dtype)
	})
	return e.fqaddr
}

// Translate uses e.Schema to write json into 'w'.
// Per the elasticsearch type specification,
// binary types are encoded to base64-encoded quoted strings.
func (e ElasticsearchDB) Translate(p []byte, b *bytes.Buffer) error { return e.Schema.WriteJSON(p, b) }

// Req returns the proper POST request to elasticsearch
func (e ElasticsearchDB) Req(r io.Reader) (hr *http.Request, err error) {
	hr, err = http.NewRequest("POST", e.Address(), r)
	return
}

// Validate returns an error if res.StatusCode is not 200 or 201
func (e ElasticsearchDB) Validate(res *http.Response) error {
	res.Body.Close()
	if res.StatusCode != 200 && res.StatusCode != 201 {
		return fmt.Errorf("[ERR] Elasticsearch (%s/%s/%s): status code %d", e.Addr, e.Index, e.Dtype, res.StatusCode)
	}
	return nil
}
