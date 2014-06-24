package main

import (
	"encoding/base64"
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"io"
	"net/http"
	"strconv"
	"sync"
)

// ElasticsearchDB conforms to the
// DB interface.
type ElasticsearchDB struct {
	Schema *msg.Schema
	Addr   string
	Index  string
	Dtype  string
	fqaddr string
	once   *sync.Once
}

// Address returns the endpoint that this db POSTs to.
func (e *ElasticsearchDB) Address() string {
	e.once.Do(func() {
		e.fqaddr = fmt.Sprintf("%s/%s/%s", e.Addr, e.Index, e.Dtype)
	})
	return e.fqaddr
}

// Translate uses e.Schema to write json into 'w'
func (e ElasticsearchDB) Translate(p []byte, w msg.Writer) error {
	var nr int //totoal number of bytes read
	var n int  //each number of bytes read
	var err error
	empty := []byte{}
	w.WriteByte(lcurly)

	for i, o := range *(e.Schema) {
		//write comma
		if i != 0 {
			w.WriteByte(comma)
		}
		w.Write(strconv.AppendQuote(empty, o.Name)) // write value name
		w.WriteByte(colon)
		// read value; write value
		switch o.T {
		case msg.String:
			var s string
			s, n, err = msg.ReadStringZeroCopy(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendQuote(empty, s))
			nr += n
			continue

		case msg.Int:
			var i int64
			i, n, err = msg.ReadIntBytes(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendInt(empty, i, 10))
			nr += n
			continue

		case msg.Uint:
			var u uint64
			u, n, err = msg.ReadUintBytes(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendUint(empty, u, 10))
			nr += n
			continue

		case msg.Bool:
			var b bool
			b, n, err = msg.ReadBoolBytes(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendBool(empty, b))
			nr += n
			continue

		case msg.Float:
			var f float64
			f, n, err = msg.ReadFloatBytes(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendFloat(empty, f, 'f', -1, 64))
			nr += n
			continue

		case msg.Bin:
			// Elasticsearch specifies base-64 encoding for binary data
			var dat []byte
			dat, n, err = msg.ReadBinZeroCopy(p[nr:])
			if err != nil {
				return err
			}
			w.Write(strconv.AppendQuote(empty, base64.StdEncoding.EncodeToString(dat)))
			nr += n
			continue
		case msg.Ext:
			var dat []byte
			var etype int8
			// Ext is the only nested object
			dat, etype, n, err = msg.ReadExtZeroCopy(p[nr:])
			if err != nil {
				return err
			}
			w.WriteByte(lcurly)
			w.Write(strconv.AppendQuote(empty, "extension_type:"))
			w.Write(strconv.AppendInt(empty, int64(etype), 10))
			w.WriteByte(comma)
			w.Write(strconv.AppendQuote(empty, "data:"))
			w.Write(strconv.AppendQuote(empty, base64.StdEncoding.EncodeToString(dat)))
			w.WriteByte(rcurly)
			nr += n
			continue

		default:
			return msg.ErrTypeNotSupported

		}

	}

	w.WriteByte(rcurly)
	return err
}

// Req returns the proper POST request to elasticsearch
func (e ElasticsearchDB) Req(r io.Reader) (hr *http.Request, err error) {
	hr, err = http.NewRequest("POST", e.Address(), r)
	return
}

// Validate returns an error if res.StatusCode is not 200 or 201
func (e ElasticsearchDB) Validate(res *http.Response) error {
	if res.StatusCode != 200 && res.StatusCode != 201 {
		return fmt.Errorf("[ERR] Elasticsearch (%s/%s/%s): status code %d", e.Addr, e.Index, e.Dtype, res.StatusCode)
	}
	return nil
}
