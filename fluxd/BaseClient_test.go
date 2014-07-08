package fluxd

import (
	"io"
	"io/ioutil"
	"net/http"
	"sync"
)

// mock http.Client type - records reqeusts
type testClient struct {
	m    *sync.Mutex
	reqs []*http.Request
}

// fulfill gclient interface
func (c *testClient) Do(req *http.Request) (res *http.Response, err error) {
	c.m.Lock()
	c.reqs = append(c.reqs, req)
	c.m.Unlock()
	res = new(http.Response)
	res.Status = "200 OK"
	res.StatusCode = 200
	res.Body = ioutil.NopCloser(*new(io.Reader))
	return
}

func (c *testClient) DumpRequests() {
	for _, r := range c.reqs {
		dump(r)
	}
}

func (c *testClient) Requests() []*http.Request { return c.reqs }
