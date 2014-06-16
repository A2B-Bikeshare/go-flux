package fluxlog

import (
	"bytes"
	"github.com/bitly/go-nsq"
	capn "github.com/glycerine/go-capnproto"
	//"github.com/philhofer/gringo"
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

type safelist struct {
	c chan *capn.Segment
}

func (s *safelist) Write(c *capn.Segment) {
	s.c <- c
}

func (s *safelist) Read() (c *capn.Segment) {
	c = <-s.c
	return
}

func newlist() *safelist { return &safelist{c: make(chan *capn.Segment, 32)} }

//Logger is the base type for sending messages to NSQ. It is a wrapper for an nsq 'Producer'
type Logger struct {
	w      *nsq.Producer
	list   *safelist
	Topic  string
	DbName string
	done   chan *nsq.ProducerTransaction
	fexit  chan sig
	wg     *sync.WaitGroup //used for waiting for conumer and error goroutines to finish
	cguard *sync.Mutex     //used for accessing logger state; immutable otherwise
	closed bool            //closed; only changed by l.Close(); must be accessed by Mutex
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
		list:   newlist(),
		Topic:  Topic,
		DbName: DbName,
		done:   make(chan *nsq.ProducerTransaction),
		fexit:  make(chan sig, 2),
		wg:     new(sync.WaitGroup),
		cguard: new(sync.Mutex),
		closed: false,
	}
	l.wg.Add(2)
	//Publish writes to the logger
	go func(l *Logger) {
		var seg *capn.Segment
		var buf *bytes.Buffer
		var err error

		//N.B. there can only be 'break's in this loop; no returns
		//otherwise l.Close() will deadlock
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
					log.Println("NSQD disconnected; attempting reconnect...")
					goto pub

				//break if the producer was stopped (somehow)
				case nsq.ErrStopped:
					//goto test; receive on <-l.fexit
					l.fexit <- sig{}
					goto test
				default:
					log.Print(err)
				}
			}

		test:
			//check for exit; get segment
			select {

			//break on receive on l.fexit
			case <-l.fexit:
				//stop the producer (flush error channel)
				l.w.Stop()
				close(l.done)
				time.Sleep(10 * time.Millisecond)
				goto exit

			//read from list; publishasync
			default:
				seg = l.list.Read()

				//test for nil signal
				// -- should be sent after
				// -- a send on l.fexit
				// otherwise, we get another seg.
				if seg == nil {
					goto test
				}

				//test for buffer initialization
				//should only be true on the 1st loop
				if buf == nil {
					buf = getBuffer()
				}

				//reset and write
				buf.Reset()
				seg.WriteTo(buf)
			}
		}
	exit:
		//Cleanup after break
		log.Println("Publish loop exited.")
		l.wg.Done()
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
		log.Println("Error loop exited.")
		l.wg.Done()
	}(l)

	return l, nil
}

//send a map[string]interface{} over the wire
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

//send a log message over the wire
func (l *Logger) doMsg(level LogLevel, message string) {
	buf := getBytes()
	seg := capn.NewBuffer(buf)
	LogMsgtoSegment(seg, l.DbName, level, message)
	l.list.Write(seg)
	putBytes(buf)
}

//determine if logger is closed.
//loggers cannot be restarted; you must call NewLogger()
func (l *Logger) IsClosed() (b bool) {
	l.cguard.Lock()
	if l.closed {
		b = true
	} else {
		b = false
	}
	l.cguard.Unlock()
	return
}

//idempotently closes the logger
//subsequent calls nop
//blocks until all cleanup is complete
func (l *Logger) Close() {
	l.cguard.Lock()
	if l.closed {
		return
	}
	l.closed = true
	l.cguard.Unlock()
	//send exit signal to writer
	l.fexit <- sig{}
	//force check for exit by sending nil
	l.list.Write(nil)
	//wait for cleanup
	l.wg.Wait()
}
