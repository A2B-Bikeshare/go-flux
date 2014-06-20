package log

import (
	"bytes"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"github.com/bitly/go-nsq"
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

func init() {
	defaultConfig = nsq.NewConfig()
	defaultConfig.Set("verbose", false)
	defaultConfig.Set("snappy", true)
	defaultConfig.Set("max_in_flight", 100)
	bufferPool = new(sync.Pool)
	bufferPool.New = func() interface{} { return bytes.NewBuffer(nil) }
}

/*	////////////////
	POOL OPERATIONS
	///////////////			*/
func getBuffer() *bytes.Buffer {
	buf, ok := bufferPool.Get().(*bytes.Buffer)
	if !ok {
		return bytes.NewBuffer(nil)
	}
	buf.Truncate(0)
	return buf
}

func putBuffer(buf *bytes.Buffer) {
	bufferPool.Put(buf)
}

/*  //////////////
		LOGGER
//////////////			*/

//Logger is the base type for sending messages to NSQ. It is a wrapper for an nsq 'Producer'
type Logger struct {
	w      *nsq.Producer
	list   chan msg.StructMessage
	Topic  string
	DbName string
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
		list:   make(chan msg.StructMessage, 8),
		Topic:  Topic,
		DbName: DbName,
		wg:     new(sync.WaitGroup),
		cguard: new(sync.Mutex),
		closed: false,
	}

	//launch publish workers
	l.wg.Add(4)
	for i := 0; i < 4; i++ {
		go publoop(l)
	}

	return l, nil
}

//publish loop
func publoop(l *Logger) {
	dones := make(chan *nsq.ProducerTransaction)
	var trans *nsq.ProducerTransaction
	var err error
	buf := getBuffer()
	buf.Grow(64)
	for msg := range l.list {
		//write message to buffer
		err = msg.Encode(buf)
		if err != nil {
			log.Printf("Message encode error: %s", err.Error())
		}
	send:
		err = l.w.PublishAsync(l.Topic, buf.Bytes(), dones, nil)
		if err != nil {
			//end if
			if err == nsq.ErrStopped {
				log.Print("publoop closing")
				goto exit
			}
			if err == nsq.ErrNotConnected {
				log.Print("NSQ producer not connected. Waiting for re-connect.")
				time.Sleep(50 * time.Millisecond)
				goto send
			}
		}

		//wait for transaction to finish; check for err
		trans = <-dones
		if trans.Error != nil {
			log.Println(err)
		}

		buf.Reset()
	}
exit:
	l.wg.Done()
}

func sendentry(e *Entry, l *Logger) {
	l.list <- e
}

//send a log message over the wire
func (l *Logger) doMsg(level LogLevel, message string) {
	e := &Entry{Level: int64(level), Message: message}
	e.Stamp()
	go sendentry(e, l)
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

// Close idepotently
func (l *Logger) Close() {
	l.cguard.Lock()
	if l.closed {
		return
	}
	l.closed = true
	l.cguard.Unlock()
	//close list; wait for workers to end.
	close(l.list)
	l.wg.Wait()
}
