package fluxd

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"io"
	"net/http"
	"strconv"
)

var (
	bprefix  = []byte("[")
	bpostfix = []byte("]")
	econcat  = []byte(",")
)

// InfluxDB implements the BatchBinding interface.
// It uses the first field in the Schema as the series name.
type InfluxDB struct {
	Schema msg.Schema
	Addr   string
	DBname string
	fqaddr string
}

// Init must be called before the call to Server.Run()
// in order to initialize unexported struct members.
func (d *InfluxDB) Init() error {
	d.fqaddr = fmt.Sprintf("%s/db/%s/series?u=root&p=root", d.Addr, d.DBname)
	return nil
}

// Address returns {Addr}/db/{DBname}/series?u=root&p=root, but
// only computes the string once.
func (d *InfluxDB) Address() string {
	return d.fqaddr
}

// Tranlsate writes InfluxDB-compatible JSON from 'p' into 'w',
// returning an error if it encounters a problem decoding the data.
// Note that the msg.PackExt type is not supported, as it cannot
// be written as "flat" data. The first value in the Schema is assumed to
// be the series name. (Any other arrangement requires a significantly more
// complicated implementation.)
func (d *InfluxDB) Translate(p []byte, w msg.Writer) error {
	// require Schema[0] to be a string
	if d.Schema[0].T != msg.String {
		return errors.New("The first member of an InfluxDB Schema must be a string.")
	}

	var stackbuf [64]byte
	stackbuf[0] = 0x2c //comma
	empty := stackbuf[1:1]
	comma := stackbuf[0:1]
	var n int
	var nr int
	var err error
	// series name
	w.WriteString("{\"name\":")
	var namestr string
	namestr, n, err = msg.ReadStringZeroCopy(p[nr:])
	if err != nil {
		return err
	}
	nr += n

	w.Write(strconv.AppendQuote(empty, namestr))

	w.WriteString(",\"columns\":[")
	//loop and write names
	for i := 1; i < len(d.Schema); i++ {
		var prepend []byte
		if i == 1 {
			prepend = empty
		} else {
			prepend = comma
		}
		w.Write(strconv.AppendQuote(prepend, d.Schema[i].Name))
	}

	// loop and write points
	w.WriteString("],\"points\":[[")
	for i := 1; i < len(d.Schema); i++ {
		var prepend []byte
		if i == 1 {
			prepend = empty
		} else {
			prepend = comma
		}
		switch d.Schema[i].T {
		case msg.String:
			var s string
			s, n, err = msg.ReadStringZeroCopy(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendQuote(prepend, s))
			nr += n
			continue

		case msg.Float:
			var f float64
			f, n, err = msg.ReadFloatBytes(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendFloat(prepend, f, 'f', -1, 64))
			nr += n
			continue

		case msg.Int:
			var i int64
			i, n, err = msg.ReadIntBytes(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendInt(prepend, i, 10))
			nr += n
			continue

		case msg.Uint:
			var u uint64
			u, n, err = msg.ReadUintBytes(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendUint(prepend, u, 10))
			nr += n
			continue

		case msg.Bool:
			var b bool
			b, n, err = msg.ReadBoolBytes(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendBool(prepend, b))
			nr += n
			continue

		case msg.Bin:
			var dat []byte
			dat, n, err = msg.ReadBinZeroCopy(p[nr:])
			if err != nil {
				return err
			}
			if i != 1 {
				w.Write(comma)
			}
			w.WriteByte('"')
			w.WriteString(base64.StdEncoding.EncodeToString(dat))
			w.WriteByte('"')
			nr += n
			continue

		default:
			return msg.ErrTypeNotSupported
		}
	}

	w.Write([]byte{']', ']', '}'})
	return nil
}

// Req resturns a POST request to d.Address() with 'r' as the body.
func (d *InfluxDB) Req(r io.Reader) (req *http.Request) {
	var err error
	req, err = http.NewRequest("POST", d.Address(), r)
	if err != nil {
		panic(err)
	}
	return
}

// Validate returns an error if the response status code is not 200 or 201.
func (d *InfluxDB) Validate(res *http.Response) error {
	res.Body.Close()
	if res.StatusCode != 200 && res.StatusCode != 201 {
		return fmt.Errorf("InfluxDB: Status Code %d", res.StatusCode)
	}
	return nil
}

// EntryPrefix returns nil
func (d *InfluxDB) EntryPrefix() []byte { return nil }

// EntryPostfix returns nil
func (d *InfluxDB) EntryPostfix() []byte { return nil }

// BatchPrefix returns '['
func (d *InfluxDB) BatchPrefix() []byte { return bprefix }

// BatchPostfix returns ']'
func (d *InfluxDB) BatchPostfix() []byte { return bpostfix }

// Concat returns ','
func (d *InfluxDB) Concat() []byte { return econcat }
