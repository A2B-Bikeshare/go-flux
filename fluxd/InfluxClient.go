package fluxd

import (
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"io"
	"net/http"
	"sync"
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
		d.fqaddr = fmt.Sprintf("%s/db/%s/series?u=root&p=root", d.Addr, d.DBname)
	})
	return d.fqaddr
}

// TODO
func (d *InfluxDB) Translate(p []byte, w msg.Writer) error {
	var nmidx int
	_ = nmidx
	// range; get 'columns' and name
	for _, o := range d.Schema {
		//
		_ = o
	}

	// range again; put points

	return nil
}

// TODO
func (d *InfluxDB) Req(r io.Reader) (req *http.Request, err error) {

	return
}

// TODO
func (d *InfluxDB) Validate(res *http.Response) error {
	if res.StatusCode != 200 && res.StatusCode != 201 {
		return fmt.Errorf("InfluxDB: Status Code %d", res.StatusCode)
	}
	return nil
}
