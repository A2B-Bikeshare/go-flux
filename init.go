package fluxlog

import (
	"github.com/bitly/go-nsq"
	capn "github.com/glycerine/go-capnproto"
	"log"
	"time"
)

var (
	defaultConfig *nsq.Config
)

func init() {
	defaultConfig = nsq.NewConfig()
	defaultConfig.Set("verbose", false)
	defaultConfig.Set("snappy", true)
	defaultConfig.Set("max_in_flight", 20)
}

type Client struct {
	w *nsq.Producer
}

func NewClient(nsqdAddr string, secret string) (*Client, error) {
	err := defaultConfig.Set("auth_secret", secret)
	if err != nil {
		return nil, err
	}

	prod := nsq.NewProducer(nsqdAddr, defaultConfig)
	return &Client{w: prod}, nil
}

type Logger struct {
	c      *Client
	Topic  string
	DbName string
}

func NewLogger(c *Client, topic string, dbname string) *Logger {
	return &Logger{
		c:      c,
		Topic:  topic,
		DbName: dbname,
	}
}

func (l *Logger) doEntry(e map[string]interface{}) {
	e["time"] = time.Now().Unix()
	//Get a buffer; create a capnproto segment
	buf := getBytes()
	seg := capn.NewBuffer(buf)
	err := EntrytoSegment(seg, l.DbName, e)
	if err != nil {
		log.Print(err)
		return
	}

	outbuf := getBuffer()
	_, err = seg.WriteToPacked(outbuf)
	if err != nil {
		log.Print(err)
		return
	}
	l.c.w.PublishAsync(l.Topic, outbuf.Bytes(), nil, nil)
	putBytes(buf)
	putBuffer(outbuf)
}

func (l *Logger) doMsg(level LogLevel, message string) {
	buf := getBytes()
	seg := capn.NewBuffer(buf)
	LogMsgtoSegment(seg, l.DbName, level, message)

	outbuf := getBuffer()
	_, err := seg.WriteToPacked(outbuf)
	if err != nil {
		log.Print(err)
		return
	}
	l.c.w.PublishAsync(l.Topic, outbuf.Bytes(), nil, nil)
	putBytes(buf)
	putBuffer(outbuf)
}
