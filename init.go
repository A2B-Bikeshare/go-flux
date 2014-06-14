package fluxlog

import (
	"bytes"
	"github.com/bitly/go-nsq"
	capn "github.com/glycerine/go-capnproto"
	"github.com/philhofer/gringo"
	"log"
	"os"
	"sync"
	"time"
)

const (
	//MAXBUFFERLENGTH determines max sustained memory usage per logger.
	//Each logger uses two message buffers.
	//Each buffer is allowed to grow unbounded, but the buffer is deleted and re-allocated
	//to a smaller one if it is greater than MAXBUFFERLENGTH.
	MAXBUFFERLENGTH = 2000
)

var (
	defaultConfig *nsq.Config
	bufferPool    *sync.Pool
	bytesPool     *sync.Pool
)

type sig struct{}

func init() {
	defaultConfig = nsq.NewConfig()
	defaultConfig.Set("verbose", false)
	defaultConfig.Set("snappy", true)
	defaultConfig.Set("max_in_flight", 100)
	bufferPool = new(sync.Pool)
	bufferPool.New = func() interface{} { return bytes.NewBuffer(getBytes()) }
	bytesPool = new(sync.Pool)
	bytesPool.New = func() interface{} { return make([]byte, 0, 300) }
}

/*	////////////////
	POOL OPERATIONS
	///////////////			*/
func getBuffer() *bytes.Buffer {
	buf, ok := bufferPool.Get().(*bytes.Buffer)
	if !ok {
		panic("Bufferpool did something weird.")
	}
	buf.Truncate(0)
	return buf
}

func putBuffer(buf *bytes.Buffer) {
	bufferPool.Put(buf)
}

func getBytes() []byte {
	bytes, ok := bytesPool.Get().([]byte)
	if !ok {
		panic("bytespool did something weird")
	}
	bytes = bytes[0:]
	return bytes
}

func putBytes(b []byte) {
	bytesPool.Put(b)
}

/*  //////////////
		LOGGER
//////////////			*/

//Logger is the base type for sending messages to NSQ. It is a wrapper for an nsq 'Producer'
type Logger struct {
	w      *nsq.Producer
	list   *gringo.Gringo
	Topic  string
	DbName string
	done   chan *nsq.ProducerTransaction
	fexit  chan sig
}

/* NewLogger returns a logger that writes data on the NSQ topic 'Topic'
and includes the field 'name' as 'DbName'. 'nsqdAddr' should be a fully-qualified
address of an nsqd instance (usually running on the same machine), and 'secret'
is the shared secret with that nsqd instance. */
func NewLogger(Topic string, DbName string, nsqdAddr string, secret string) (*Logger, error) {
	if secret != "" {
		err := defaultConfig.Set("auth_secret", secret)
		if err != nil {
			return nil, err
		}
	}
	prod := nsq.NewProducer(nsqdAddr, defaultConfig)
	prod.SetLogger(log.New(os.Stdout, "", 0), nsq.LogLevelDebug)
	l := &Logger{
		w:      prod,
		list:   gringo.NewGringo(),
		Topic:  Topic,
		DbName: DbName,
		done:   make(chan *nsq.ProducerTransaction, 16),
		fexit:  make(chan sig, 1),
	}

	//Publish writes to the logger
	go func(l *Logger) {
		var seg *capn.Segment
		var buf *bytes.Buffer
		var err error
		for {
			//send message FIRST, but check for sanity
			if buf == nil || buf.Len() == 0 {
				goto test
			}

		pub:
			//publish segment; check for connection
			err = l.w.PublishAsync(l.Topic, buf.Bytes(), l.done, nil)
			if err != nil {
				switch err {
				//if not connected, wait for reconnection, loop back
				case nsq.ErrNotConnected:
					time.Sleep(50 * time.Millisecond)
					goto pub

				//break if the producer was stopped
				case nsq.ErrStopped:
					close(l.done)
					break
				default:
					log.Print(err)
				}
			}
			//recycle buffer
			putBuffer(buf)

		test:
			//check for exit; get segment
			select {

			//break on receive on fexit
			case <-l.fexit:
				break

			//read from list; publishasync
			default:
				seg = l.list.Read()
				//check for nonsense
				if seg == nil {
					goto test
				}

				buf = getBuffer()
				seg.WriteTo(buf)
			}
		}
		return
	}(l)

	//Log errors from failed pubs
	go func(l *Logger) {
		var pd *nsq.ProducerTransaction
		for pd = range l.done {
			if pd.Error != nil {
				log.Printf("Encountered error %q publishing to nsq with args %#v", pd.Error, pd.Args)
			}
		}
	}(l)

	return l, nil
}

func (l *Logger) doEntry(e map[string]interface{}) (err error) {
	e["time"] = time.Now().Unix()
	//Get a buffer; create a capnproto segment
	buf := getBytes()
	seg := capn.NewBuffer(buf)
	err = EntrytoSegment(seg, l.DbName, e)
	if err != nil {
		return
	}
	l.list.Write(seg)
	putBytes(buf)
	return
}

func (l *Logger) doMsg(level LogLevel, message string) {
	buf := getBytes()
	seg := capn.NewBuffer(buf)
	LogMsgtoSegment(seg, l.DbName, level, message)
	l.list.Write(seg)
	putBytes(buf)
}

func (l *Logger) Close() {
	//send exit signal to writer
	l.fexit <- sig{}
	//force check for exit by sending nil
	l.list.Write(nil)
	//stop the producer
	l.w.Stop()
	//stop the error channel
	close(l.done)
}
