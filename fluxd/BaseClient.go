package main

import (
	"bytes"
	"github.com/bitly/go-nsq"
	"github.com/philhofer/fluxlog"
	"net/http"
	"sync"
)

/*
	Buffer Pool Logic
	- Get()
	- Put()
*/
type bufPool struct {
	pool *sync.Pool
}

func (b *bufPool) Get() *bytes.Buffer {
	buf, ok := b.pool.Get().(*bytes.Buffer)
	if !ok {
		return bytes.NewBuffer(nil)
	}
	buf.Reset()
	return buf
}

func (b *bufPool) Put(buf *bytes.Buffer) {
	b.pool.Put(buf)
}

//DB is the interface all databases must satisfy
type DB interface {
	//Request should return the proper http request to make given
	//'saddr' as the database address, 'topic' as the nsq topic string,
	//and a buffer that contains the decoded message body
	Request(saddr string, topic string, b *bytes.Buffer) (req *http.Request, err error)
	//Validate should return nil if the response
	//indicates success on file upload, and an error otherwise
	Validate(res *http.Response) (err error)
	//DB should satisfy the fluxlog.Decoder interface
	fluxlog.Decoder
}

type dbconn struct {
	*http.Client
	db    DB
	topic string
	saddr string
	con   *nsq.Consumer
}

//dbconn implements the nsq.Handler interface
func (d *dbconn) HandleMessage(m *nsq.Message) error {
	//write message body to buffer using decoder
	buf := bpl.Get()
	err := fluxlog.UseDecoder(d.db, m.Body, buf)
	if err != nil {
		return err
	}

	//make http request
	req, err := d.db.Request(d.saddr, d.topic, buf)
	if err != nil {
		return err
	}

	//do http request
	res, err := d.Do(req)
	if err != nil {
		return err
	}
	bpl.Put(buf)
	return d.db.Validate(res)
}
