package main

import (
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"net/http"
	"sync"
)

// ElasticsearchDB conforms to the
// DB interface.
type ElasticsearchDB struct {
	Addr   string
	Index  string
	Dtype  string
	fqaddr string
	once   *sync.Once
}

func (e *ElasticsearchDB) Address() string {
	e.once.Do(func() {
		e.fqaddr = fmt.Sprintf("%s/%s/%s", e.Addr, e.Index, e.Dtype)
	})
	return e.fqaddr
}

// TODO
func (e ElasticsearchDB) Translate(r msg.Reader, w io.Writer) error {
	//TODO
	return nil
}

func (e ElasticsearchDB) Req(r io.Reader) (r *http.Request, err error) {
	r, err = http.NewRequest("POST", e.Address(), r)
	return
}

func (e ElasticsearchDB) Validate(res *http.Response) error {
	if res.StatusCode != 200 && res.StatusCode != 201 {
		return fmt.Errorf("[ERR] Elasticsearch (%s/%s/%s): status code %d", e.Addr, e.Index, e.Dtype, res.StatusCode)
	}
}
