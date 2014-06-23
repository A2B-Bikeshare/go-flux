package log

import (
	"bytes"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"github.com/bitly/go-nsq"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var (
	defaultConfig *nsq.Config

	// publisher duration by number (ms) - time idle before closing
	// 60m, 15m, 4m, 1m, 15s, 4s, 1s, 250ms
	pubDurs = [maxPubs]int64{60 * 60 * 1000, 15 * 60 * 1000, 4 * 60 * 1000, 60 * 1000, 15 * 1000, 4 * 1000, 1000, 250}
)

const (
	// number of connection retries
	// each publishloop attempts
	// before exiting fatally
	nsqRetries = 5
	// maximum number of publishers
	maxPubs = 8
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
	status int64 // 0 stopped; 1 running; others for future use (?)
	npubs  int64 // number of publishers running
	// Topic is the topic that the logger writes on - should only be set by NewLogger
	Topic string
	w     *nsq.Producer
	wg    *sync.WaitGroup  // used for waiting for consumer and error goroutines to finish
	swg   *sync.WaitGroup  // used for waiting on async sends to prevent sends on a closed channel
	list  chan msg.Encoder // used for messages
}

// NewLogger returns a logger that writes data on the NSQ topic 'Topic.'
// 'nsqdAddr' should be the address of an nsqd instance (usually running on the same machine), and 'secret'
// is the shared secret with that nsqd instance (can be "" for none.)
func NewLogger(Topic string, nsqdAddr string, secret string) (*Logger, error) {
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
	l := &Logger{
		status: 1,
		npubs:  0,
		Topic:  Topic,
		w:      prod,
		wg:     new(sync.WaitGroup),
		swg:    new(sync.WaitGroup),
		list:   make(chan msg.Encoder, 64),
	}

	return l, nil
}

// publish loop:
// each publish loop continuously pops
// msg.Encoders off of l.list, writes to
// a byte array, and publishes that data to NSQ.
// Loops timeout (return) after not receiving for 'dur' time
func publoop(l *Logger, dur time.Duration) {
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
			// exit on channel close
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
				log.Printf("ERROR: flux/log: Couldn't connect to NSQ after %d retries. Closing publoop.", retries)
				log.Printf("ERROR: flux/log: Couldn't send message %v", msg)
				goto exit
			}
			if err != nil {
				//end if
				if err == nsq.ErrStopped {
					//exit on permanently stopped worker
					goto exit
				} else if err == nsq.ErrNotConnected {
					// deal with lazy connecting/disconnecting
					log.Print("INFO: flux/log: NSQ producer not connected. Retrying...")
					retries++
					time.Sleep(20 * time.Millisecond)
					goto send
				} else {
					// unknown error
					log.Printf("ERROR: flux/log: %s", err.Error())
				}
			} else {
				retries = 0
				// log transaction errors
				trans = <-dones
				if trans.Error != nil {
					log.Println(err)
				}
			}

			// always reset buffer on continue
			buf.Reset()
		}

	}
exit:
	_ = atomic.AddInt64(&l.npubs, -1)
	l.wg.Done()
}

// add a publisher worker
func (l *Logger) addworker() {
	// don't add if done
	if atomic.LoadInt64(&l.status) == 0 {
		return
	}
	// don't add after maxPubs
	if atomic.LoadInt64(&l.npubs) >= maxPubs {
		return
	}
	l.wg.Add(1)
	np := atomic.AddInt64(&l.npubs, 1)
	// check sanity; npubs may have changed (unlikely but possible)
	// pubDurs[np-1] will panic if np is too large, so this is critical
	if np > maxPubs {
		// ABORT
		_ = atomic.AddInt64(&l.npubs, -1)
		l.wg.Done()
		return
	}
	log.Printf("INF: flux/log: Starting publisher %d", np-1)
	go publoop(l, time.Duration(pubDurs[np-1])*time.Millisecond)
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
	// check status; return if stopped
	if atomic.LoadInt64(&l.status) == 0 {
		return
	}
	// ensure at least one worker running
	if atomic.LoadInt64(&l.npubs) == 0 {
		// don't go through the usual send process,
		// or we'll start TWO new workers, because
		// the send won't immediately succeed.
		l.addworker()
		l.swg.Add(1)
		select {
		case l.list <- e:
		case <-time.After(5 * time.Second):
			log.Printf("ERROR: flux/log: timeout; couldn't send message %v", e)
		}
		l.swg.Done()
		return
	}

	// register async send
	l.swg.Add(1)
	select {
	case l.list <- e:
	default:
		// add capacity if we're backed up
		l.addworker()
		l.list <- e
	}
	l.swg.Done()
	return
}

// IsClosed returns the state of the logger.
// The logger cannot be 're-opened' onced closed.
func (l *Logger) IsClosed() bool {
	if atomic.LoadInt64(&l.status) == 0 {
		return true
	}
	return false
}

// Workers returns the number of publisher goroutines
// running concurrently. (Between zero and eight, dynamically
// adjusted based on message frequency.)
func (l *Logger) Workers() int64 {
	return atomic.LoadInt64(&l.npubs)
}

// Close permanently closes the logger
func (l *Logger) Close() {
	if !atomic.CompareAndSwapInt64(&l.status, 1, 0) {
		//already closed
		return
	}
	// wait for async sends to end
	l.swg.Wait()
	// close channel
	close(l.list)
	// wait for pubs to end
	l.wg.Wait()
	// stop producer
	l.w.Stop()
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
			if atomic.LoadInt64(&l.status) == 0 {
				c <- e
				break
			}
			l.doEncoder(e)
		}
	}(l)
}
