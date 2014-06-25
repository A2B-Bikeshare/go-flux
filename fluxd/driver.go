package fluxd

import (
	"errors"
	"github.com/bitly/go-nsq"
	"net/http"
)

var (
	// DefaultConfig is the default configuration used for NSQ communications by each binding if one is not specified.
	DefaultConfig *nsq.Config
)

func init() {
	DefaultConfig = nsq.NewConfig()
	_ = DefaultConfig.Set("max_in_flight", 25)
}

// Server represents a collection of bindings
// to an NSQlookupd instance.
type Server struct {
	// Config used for NSQ connections; defaults to DefaultConfig
	NSQConfig *nsq.Config
	// Address(es) of nsqlookupd(s)
	Lookupdaddrs []string
	bindings     []*Binding
}

// Stop ends all of the server processes gracefully. It does not block.
func (s *Server) Stop() {
	for _, b := range s.bindings {
		go func(b *Binding) {
			b.cons.Stop()
			return
		}(b)
	}
}

// Bind adds a binding to the server. Bind must be called before Run.
func (s *Server) Bind(b *Binding) { s.bindings = append(s.bindings, b) }

// use server config and addrs to configure a binding
// and start the consumer, client, and handler
func (s *Server) startrun(b *Binding) error {
	var err error
	b.cons, err = nsq.NewConsumer(b.Topic, b.Channel, s.NSQConfig)
	if err != nil {
		return err
	}

	err = b.cons.ConnectToNSQLookupds(s.Lookupdaddrs)
	if err != nil {
		return err
	}

	// default client
	b.dcl = &http.Client{}
	if b.Workers <= 0 {
		b.Workers = 1
	}
	b.cons.SetConcurrentHandlers(b, b.Workers)
	return nil
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

	if s.bindings == nil || len(s.bindings) == 0 {
		return errors.New("No bindings registered.")
	}

	for _, b := range s.bindings {
		err = s.startrun(b)
		if err != nil {
			return err
		}
	}
	// block until graceful stop
	for _, b := range s.bindings {
		<-b.cons.StopChan
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
	dcl     *http.Client  //used for database communication
	cons    *nsq.Consumer //used for nsq communication
}

// HandleMessage implements the nsq.Handler interface
func (b *Binding) HandleMessage(msg *nsq.Message) error {
	return dbHandle(b.Endpoint, msg.Body, b.dcl)
}
