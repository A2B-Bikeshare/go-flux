package fluxd

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
)

// stdout client
type stdoutcl struct{}

func dump(r *http.Request) {
	body, _ := httputil.DumpRequest(r, true)
	fmt.Println("--------- SERVER REQUEST ---------")
	fmt.Printf("%s\n", body)
	fmt.Println("----------------------------------")
}

// fulfill stdout interface
func (s stdoutcl) Do(req *http.Request) (*http.Response, error) {
	dump(req)
	res := new(http.Response)
	res.Status = "200 OK"
	res.StatusCode = 200
	res.Body = ioutil.NopCloser(*new(io.Reader))
	return res, nil
}
