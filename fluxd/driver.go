package fluxd

import (
	"bytes"
	"errors"
	"github.com/bitly/go-nsq"
	"log"
	"net/http"
	"sync"
	"time"
)

var (
	// DefaultConfig is the default configuration used for NSQ communications by each binding if one is not specified.
	DefaultConfig *nsq.Config
)

func init() {
	DefaultConfig = nsq.NewConfig()
	_ = DefaultConfig.Set("max_in_flight", 50)
}

// Server represents a collection of bindings
// to NSQLookupd instances.
type Server struct {
	// Config used for NSQ connections; defaults to DefaultConfig
	NSQConfig *nsq.Config
	// Address(es) of nsqlookupd(s)
	Lookupdaddrs []string
	// If true, requests are redirected to StdOut instead of
	// to the http endpoint. Useful for testing.
	UseStdout bool
	bindings  []*Binding
	batchbin  []*BatchBinding
}

// Stop ends all of the server processes gracefully. It does not block.
func (s *Server) Stop() {
	for _, b := range s.bindings {
		go func(b *Binding) {
			b.cons.Stop()
			return
		}(b)
	}
	for _, bb := range s.batchbin {
		go func(bb *BatchBinding) {
			bb.cons.Stop()
			bb.stchan <- struct{}{}
			return
		}(bb)
	}
}

// Bind adds a binding to the server. Bind must be called before Run.
func (s *Server) Bind(b *Binding) { s.bindings = append(s.bindings, b) }

// BindBatch adds a batched binding to the server. Bind must be called before Run.
func (s *Server) BindBatch(b *BatchBinding) { s.batchbin = append(s.batchbin, b) }

// use server config and addrs to configure a binding
// and start the consumer, client, and handler
func (s *Server) startrun(b *Binding) error {
	var err error
	err = b.Endpoint.Init()
	if err != nil {
		return err
	}

	b.cons, err = nsq.NewConsumer(b.Topic, b.Channel, s.NSQConfig)
	if err != nil {
		return err
	}


	if !s.UseStdout {
		// default client
		b.dcl = &http.Client{}
		if b.Workers <= 0 {
			b.Workers = 1
		}
	} else {
		// use stdout
		b.dcl = stdoutcl{}
		b.Workers = 1
	}
	b.cons.AddConcurrentHandlers(nsq.HandlerFunc(b.handle), b.Workers)
	err = b.cons.ConnectToNSQLookupds(s.Lookupdaddrs)
	return err
}

func (s *Server) startbatch(b *BatchBinding) error {
	var err error
	err = b.Endpoint.Init()
	if err != nil {
		return err
	}

	b.cons, err = nsq.NewConsumer(b.Topic, b.Channel, s.NSQConfig)
	if err != nil {
		return err
	}

	if !s.UseStdout {
		b.dcl = &http.Client{}
		if b.Workers < 1 {
			b.Workers = 1
		}
	} else {
		b.dcl = stdoutcl{}
		b.Workers = 1
	}
	if b.MaxMsg <= 0 {
		b.MaxMsg = 50
	}
	if b.BatchTime == 0 {
		b.BatchTime = 250 * time.Millisecond
	}

	b.outbuf = bytes.NewBuffer(nil)
	b.outbuf.Grow(2048)
	b.accum = make(chan *bytes.Buffer, 128)
	b.stchan = make(chan struct{}, 1)

	b.wg = new(sync.WaitGroup)
	b.wg.Add(1)
	go batchloop(b, b.stchan)

	b.cons.AddConcurrentHandlers(nsq.HandlerFunc(b.handle), b.Workers)
	err = b.cons.ConnectToNSQLookupds(s.Lookupdaddrs)
	return err
}

func batchloop(b *BatchBinding, stchan chan struct{}) {
	var inbuf *bytes.Buffer
	var nmsg int
	var ok bool
	b.outbuf.Write(b.Endpoint.BatchPrefix())
	for {
		select {
		case <-time.After(b.BatchTime):
			// can't send if zero
			if nmsg == 0 {
				continue
			}
			// postfix
			b.outbuf.Write(b.Endpoint.BatchPostfix())
			// do request
			res, err := b.dcl.Do(b.Endpoint.Req(b.outbuf))
			if err != nil {
				log.Printf("HTTP Client error: %s", err.Error())
				log.Printf("Failed to send body: %q", b.outbuf.String())
				b.outbuf.Reset()
				nmsg = 0
				b.outbuf.Write(b.Endpoint.BatchPrefix())
				continue
			}
			// validate
			err = b.Endpoint.Validate(res)
			if err != nil {
				log.Printf("Response invalidated: %s", err.Error())
				log.Printf("The following body failed: %q", b.outbuf.String())
				b.outbuf.Reset()
				nmsg = 0
				b.outbuf.Write(b.Endpoint.BatchPrefix())
				continue
			}
			// everything worked ok
			b.outbuf.Reset()
			nmsg = 0
			b.outbuf.Write(b.Endpoint.BatchPrefix())
			continue

		case inbuf, ok = <-b.accum:
			if !ok {
				goto exit
			}
			// write concatenator
			if nmsg != 0 {
				b.outbuf.Write(b.Endpoint.Concat())
			}
			// write prefix, msg, postfix
			b.outbuf.Write(b.Endpoint.EntryPrefix())
			inbuf.WriteTo(b.outbuf)
			b.outbuf.Write(b.Endpoint.EntryPostfix())
			// free inbuf
			putBuf(inbuf)
			// increment msg; see if we have hit maxmsg
			nmsg++
			if nmsg >= b.MaxMsg {
				// BATCH SEND

				// batch postfix
				b.outbuf.Write(b.Endpoint.BatchPostfix())
				//send everything
				res, err := b.dcl.Do(b.Endpoint.Req(b.outbuf))
				if err != nil {
					log.Printf("HTTP Client error: %s", err.Error())
					log.Printf("Failed to send body: %q", b.outbuf.String())
					b.outbuf.Reset()
					nmsg = 0
					b.outbuf.Write(b.Endpoint.BatchPrefix())
					continue
				}
				err = b.Endpoint.Validate(res)
				if err != nil {
					log.Printf("Response invalidated: %s", err.Error())
					log.Printf("The following body failed: %q", b.outbuf.String())
					b.outbuf.Reset()
					nmsg = 0
					b.outbuf.Write(b.Endpoint.BatchPrefix())
					continue
				}
				// everything worked okay
				b.outbuf.Reset()
				nmsg = 0
				b.outbuf.Write(b.Endpoint.BatchPrefix())
				continue
			}
		case <-stchan:
			goto exit
		}
	}
exit:
	b.wg.Done()
	return
}

// Run blocks until all bindings exit gracefully, usually after a call to Stop.
// Run immediately returns an error if the server is not configured correctly.
func (s *Server) Run() error {
	var err error
	if s.NSQConfig == nil {
		s.NSQConfig = DefaultConfig
	}
	err = s.NSQConfig.Validate()
	if err != nil {
		return err
	}

	if len(s.bindings) == 0 && len(s.batchbin) == 0 {
		return errors.New("No bindings registered.")
	}

	for _, b := range s.bindings {
		err = s.startrun(b)
		if err != nil {
			return err
		}
	}

	for _, b := range s.batchbin {
		err = s.startbatch(b)
		if err != nil {
			return err
		}
	}
	// block until graceful stop
	for _, b := range s.bindings {
		<-b.cons.StopChan
	}
	for _, bb := range s.batchbin {
		<-bb.cons.StopChan
		bb.wg.Wait()
	}
	return nil
}

// Binding types connect NSQ channels, flux/msg schemas, and database endpoints.
type Binding struct {
	// Topic is the NSQ topic to listen on
	Topic string
	// Channel is the NSQ channel to listen on
	Channel string
	// Endpoint is the database and decode logic used for this topic & channel
	Endpoint DB
	// Workers sets the number of concurrent goroutines serving this binding; defaults to 1
	Workers int
	dcl     dclient       //used for database communication
	cons    *nsq.Consumer //used for nsq communication
}

// BatchBinding types connect NSQ channels, flux/msg schemas, and database endpoints,
// but they use database request batching.
type BatchBinding struct {
	//Topic is the NSQ topic to listen on
	Topic string

	// Channel is the NSQ channel to listen on
	Channel string

	// Endpoint is the database and decode logic
	Endpoint BatchDB

	// Workers sets the number of concurent goroutines serving this binding; defaults to 1
	Workers int

	// MaxMsg sets the maximum number of messages collected before upload; defaults to 50
	MaxMsg int

	// BatchTime sets the maximum time spend waiting to collect messages before upload; defaults to 250ms
	BatchTime time.Duration

	dcl    dclient            // client
	cons   *nsq.Consumer      // consumer
	outbuf *bytes.Buffer      // for request body
	accum  chan *bytes.Buffer // for accumulating responses
	stchan chan struct{}      // stop channel
	wg     *sync.WaitGroup    // for monitoring workers
}

// implements the nsq.HandleFunc interface
func (b *Binding) handle(msg *nsq.Message) error {
	return dbHandle(b.Endpoint, msg.Body, b.dcl)
}

// implements the nsq.HandleFunc interface
func (b *BatchBinding) handle(msg *nsq.Message) error {
	buf := getBuf()
	err := b.Endpoint.Translate(msg.Body, buf)
	if err != nil {
		putBuf(buf)
		return err
	}
	b.accum <- buf
	return nil
}
