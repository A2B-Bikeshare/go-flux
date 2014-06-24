package fluxd

import (
	"bytes"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"io"
	"net/http"
	"sync"
)

// common UTF-8 bytes for json writing; useful for w.WriteByte
var (
	comma  byte = 0x2c //','
	colon  byte = 0x3a //':'
	lcurly byte = 0x7b //'{'
	rcurly byte = 0x7d //'}'
	bpl    *sync.Pool
)

func init() {
	bpl = new(sync.Pool)
	bpl.New = func() interface{} { return bytes.NewBuffer(make([]byte, 0, 128)) }
}

func getBuf() *bytes.Buffer {
	buf, ok := bpl.Get().(*bytes.Buffer)
	if !ok || buf == nil {
		return bytes.NewBuffer(make([]byte, 0, 128))
	}
	return buf
}

func putBuf(b *bytes.Buffer) {
	bpl.Put(b)
}

// DB is the interface that fluxd uses to communicate with a database.
// All calls should be thread-safe.
type DB interface {
	// Translate should turn the body of an NSQ message
	// into a valid JSON body to be used to write
	// the body to the database.
	Translate(p []byte, w msg.Writer) error
	// Req should return a valid *http.Request to be performed
	// by an http client. 'r' should be used
	// as the body of the request.
	Req(r io.Reader) (*http.Request, error)
	// Validate is used to validate a response from a server
	// after data is sent. Validate() should return
	// a non-nil error to mark the response as failed.
	// Validate is also responsible for closing the response
	// body.
	Validate(*http.Response) error
}

// drives database decoding & writing
// r contains nsq.Message.Body
func dbHandle(db DB, r []byte, dcl *http.Client) error {
	buf := getBuf()
	err := db.Translate(r, buf)
	if err != nil {
		return err
	}
	// make request
	req, err := db.Req(buf)
	if err != nil {
		return err
	}
	// do request; get response
	res, err := dcl.Do(req)
	putBuf(buf)
	if err != nil {
		return err
	}
	// validate response
	err = db.Validate(res)
	if err != nil {
		return err
	}
	return nil
}
