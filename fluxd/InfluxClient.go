package fluxd

import (
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"sync"
  "io"
  "net/http"
)

// TODO
type InfluxDB struct {
	Schema          msg.Schema
	Addr            string
	DBname          string
	SeriesNameField string
	fqaddr          string
	once            *sync.Once
}

// TODO
func (d *InfluxDB) Address() string {
	d.once.Do(func() {
		d.fqaddr = fmt.Sprintf("%s/db/%s/series?u=root&p=root")
	})
	return d.fqaddr
}

// TODO
func (d *InfluxDB) Translate(p []byte, w msg.Writer) error {

  return nil
}

// TODO
func (d *InfluxDB) Req(r io.Reader) (req *http.Request, err error) {

  return
}

// TODO
func (d *InfluxDB) Validate(res *http.Response) error {

  return nil
}
