package fluxd

import (
	"github.com/bitly/go-nsq"
	"net/http"
	"sync"
)

// Server represents a collection of bindings
// to an NSQlookupd instance.
type Server struct {
	// Config used for NSQ connections
	NSQConfig *nsq.Config
	// Address(es) of nsqlookupd(s)
	Lookupdaddrs []string
	bindings     []*Binding
}

// Stop ends all of the server processes gracefully, and blocks until completion.
// (This may take up to 30 seconds.)
func (s *Server) Stop() {
	wg := new(sync.WaitGroup)
	wg.Add(len(s.bindings))
	for _, b := range s.bindings {
		go func(b *Binding, wg *sync.WaitGroup) {
			b.cons.Stop()
			<-b.cons.StopChan
			wg.Done()
			return
		}(b, wg)
	}
	wg.Wait()
}

// use server config and addrs to configure a binding
// and start the consumer, client, and handler
func (s *Server) startrun(b *Binding) error {

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
	dcl      *http.Client  //used for database communication
	cons     *nsq.Consumer //used for nsq communication
}
