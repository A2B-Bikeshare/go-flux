package fluxd

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

// stdout client
type stdoutcl struct{}

func dump(r *http.Request) {
	fmt.Println("--------- SERVER REQUEST ---------")
	fmt.Printf("Method: %s\n", r.Method)
	fmt.Printf("Address: %s\n", r.URL.String())
	body, _ := ioutil.ReadAll(r.Body)
	fmt.Printf("Body:\n%s\n", body)
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
