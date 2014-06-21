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

var (
	defaultConfig *nsq.Config
)

const (
	// number of connection retries
	// each publishloop attempts
	// before exiting fatally
	nsqRetries = 5
)

func init() {
	defaultConfig = nsq.NewConfig()
	defaultConfig.Set("verbose", false)
	defaultConfig.Set("max_in_flight", 100)
}

// Logger is the base type for sending messages to NSQ. It is a wrapper for an NSQ Producer.
// Logger sends types that conform to the msg.Encoder interface on the NSQ topic equal to Topic. Logger dynamically
// starts and stops 'worker' goroutines that carry out serialization and publishing based
// on the data load. One 'worker' is always running; other workers are spawned when more than
// 32 messages sit in the logger message queue. Extra workers close after one minute of inactivity.
// Writes on the logger never block. Users should avoid writes on a closed logger.
type Logger struct {
	// Topic is the topic that the logger writes on - should only be set by NewLogger
	Topic  string
	w      *nsq.Producer
	wg     *sync.WaitGroup //used for waiting for conumer and error goroutines to finish
	cguard *sync.Mutex     //used for accessing logger state; immutable otherwise
	list   chan msg.Encoder
	closed bool //closed; only changed by l.Close(); must be accessed by lock/atomic
}

// NewLogger returns a logger that writes data on the NSQ topic 'Topic'
// and includes the field 'name' as 'DbName'. 'nsqdAddr' should be the
// address of an nsqd instance (usually running on the same machine), and 'secret'
// is the shared secret with that nsqd instance (can be "" for none.)
func NewLogger(Topic string, DbName string, nsqdAddr string, secret string) (*Logger, error) {
	if secret != "" {
		err := defaultConfig.Set("auth_secret", secret)
		if err != nil {
			return nil, err
		}
	}

	prod, err := nsq.NewProducer(nsqdAddr, defaultConfig)
	if err != nil {
		return nil, err
	}
	prod.SetLogger(log.New(os.Stdout, "", 0), nsq.LogLevelDebug)
	l := &Logger{
		w:      prod,
		list:   make(chan msg.Encoder, 32),
		Topic:  Topic,
		wg:     new(sync.WaitGroup),
		cguard: new(sync.Mutex),
		closed: false,
	}

	//launch persistent publish worker
	l.wg.Add(1)
	go publoop(l, 1000000*time.Hour)

	return l, nil
}

// publish loop:
// each publish loop continuously pops
// msg.Encoders off of l.list, writes to
// a byte array, and publishes that data to NSQ.
// Loops timeout (return) after not receiving for 'dur' time
func publoop(l *Logger, dur time.Duration) {
	log.Print("flux/log: Publoop started.")
	dones := make(chan *nsq.ProducerTransaction)
	var trans *nsq.ProducerTransaction
	var err error
	var retries int
	buf := bytes.NewBuffer(nil)
	//pre-emptively allocate some space
	buf.Grow(128)

	for {
		select {
		//quit after dur
		case <-time.After(dur):
			goto exit

		case msg, ok := <-l.list:
			if !ok {
				goto exit
			}
			//write message to buffer
			err = msg.Encode(buf)
			if err != nil {
				log.Printf("flux/log: Message encode error: %s", err.Error())
			}
		send:
			err = l.w.PublishAsync(l.Topic, buf.Bytes(), dones, nil)
			if retries > nsqRetries {
				log.Printf("flux/log: Couldn't connect to NSQ after %d retries. Closing publoop.", retries)
				log.Printf("ERROR: flux/log: couldn't send message %v", msg)
			}
			if err != nil {
				//end if
				if err == nsq.ErrStopped {
					goto exit
				}
				// NSQ 'Producer' connects lazily; retry on ErrNotConnected
				if err == nsq.ErrNotConnected {
					log.Print("flux/log: NSQ producer not connected. Retrying...")
					retries++
					time.Sleep(200 * time.Millisecond)
					goto send
				}
			}
			retries = 0

			// wait for transaction to finish; check for err
			// preferable to synchronous publishing if for
			// no other reason than to avoid channel initialization cost.
			trans = <-dones
			if trans.Error != nil {
				log.Println(err)
			}

			buf.Reset()
		}

	}
exit:
	log.Println("flux/log: Publoop exiting.")
	l.wg.Done()
}

// push entry onto stack
// after starting a new publoop
func sendencoder(e msg.Encoder, l *Logger) {
	l.wg.Add(1)
	go publoop(l, 1*time.Minute)
	select {
	case l.list <- e:
		return
	case <-time.After(5 * time.Second):
		log.Printf("ERROR: flux/log: message %v not sent", e)
	}
}

//send a log message over the wire
func (l *Logger) doMsg(level LogLevel, message string) {
	e := &Entry{Level: int64(level), Message: message}
	e.Stamp()
	l.doEncoder(e)
}

// put an encoder on the list channel, or start
// a new publoop
func (l *Logger) doEncoder(e msg.Encoder) {
	select {
	case l.list <- e:
		return
	default:
		go sendencoder(e, l)
	}
}

// IsClosed returns the state of the logger.
// The logger cannot be 're-opened'.
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

// Close permanently closes the logger
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
	return
}

// Send writes a msg.Encoder to the channel
func (l *Logger) Send(m msg.Encoder) {
	l.doEncoder(m)
}

// Listen writes messages to the logger from a channel of msg.Encoders
func (l *Logger) Listen(c chan msg.Encoder) {
	go func(l *Logger) {
		for e := range c {
			l.doEncoder(e)
		}
	}(l)
}
