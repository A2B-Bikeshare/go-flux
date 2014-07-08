package fluxd

import (
	"bytes"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"io"
	"net/http"
	"sync"
)

// common UTF-8 bytes for json writing; useful for w.WriteByte
const (
	comma  byte = 0x2c //','
	colon  byte = 0x3a //':'
	lcurly byte = 0x7b //'{'
	rcurly byte = 0x7d //'}'
	lsqr   byte = 0x5b //'['
	rsqr   byte = 0x5d //']'
)

var (
	bpl *sync.Pool
)

// for testing, client can be something other than http.Client
type dclient interface {
	Do(*http.Request) (*http.Response, error)
}

func init() {
	bpl = new(sync.Pool)
	bpl.New = func() interface{} { return bytes.NewBuffer(make([]byte, 0, 128)) }
}

func getBuf() *bytes.Buffer {
	buf, ok := bpl.Get().(*bytes.Buffer)
	if !ok || buf == nil {
		return bytes.NewBuffer(make([]byte, 0, 128))
	}
	buf.Reset()
	return buf
}

func putBuf(b *bytes.Buffer) {
	bpl.Put(b)
}

// DB is the interface that fluxd uses to communicate with a database.
// All calls should be thread-safe.
type DB interface {
	// Init gives you an opportunity to do custom initialization and
	// validation on the DB. Calls to Server.Run will cause the
	// driver to call Init() exactly once.
	Init() error

	// Translate should turn the body of an NSQ message
	// into a valid body to be used to write
	// to the database.
	Translate(p []byte, w msg.Writer) error

	// Req should return a valid *http.Request to be performed
	// by an http client. 'r' should be used
	// as the body of the request.
	Req(r io.Reader) *http.Request

	// Validate is used to validate a response from a server
	// after data is sent. Validate() should return
	// a non-nil error to mark the response as failed.
	// Validate is also responsible for closing the response
	// body.
	Validate(*http.Response) error
}

// BatchDB represents a database connection
// that handles batch uploads. XxxPrefix and XxxPostfix
// methods are used to format the body.
type BatchDB interface {
	// A BatchDB must fulfill the DB interface. The Req
	// method should point the request towards the batch
	// endpoint of the database, if it is different
	// from the standard endpoint.
	DB
	// EntryPrefix is written immediately before each individual entry
	EntryPrefix() []byte

	// EntryPostfix is written immediately after each individual entry
	EntryPostfix() []byte

	// BatchPrefix is written at the beggining of the request body
	// (before all entries, e.g. '[')
	BatchPrefix() []byte

	// BatchPostfix is written at the end of the request body
	// (after all entries, e.g. ']')
	BatchPostfix() []byte

	// Concat is placed between every entry
	// (e.g. ',')
	Concat() []byte
}

// synchronous handler for non-batched databases
func dbHandle(db DB, r []byte, dcl dclient) error {
	buf := getBuf()
	err := db.Translate(r, buf)
	if err != nil {
		return err
	}
	// make request
	req := db.Req(buf)

	// do request; get response
	res, err := dcl.Do(req)
	putBuf(buf) //we must be done with buffer
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
