// +build nsq

package log

import (
	"bytes"
	"github.com/bitly/go-nsq"
	"math/rand"
	"testing"
	"time"
)

var (
	MSGS   = []string{"This is debug info.", "This is info info.", "This is a warning.", "This is an error...", "This is FATALLLL"}
	LEVELS = []int64{0, 1, 2, 3, 4}
)

type Msg struct {
	Level   int64
	Message string
}

func getMsg() Msg {
	n := rand.Intn(5)
	return Msg{Level: LEVELS[n], Message: MSGS[n]}
}

//get n random messages
func getMsgs(n int) []Msg {
	var m Msg
	out := make([]Msg, n)
	for i := 0; i < n; i++ {
		m = getMsg()
		out[i] = m
	}
	return out
}

func logMsgs(l *Logger, msgs []Msg) {
	for _, msg := range msgs {
		l.log(msg.Level, msg.Message)
	}
}

func TestLogMessage(t *testing.T) {
	rand.Seed(time.Now().Unix())
	NMSG := 5   //number of messages sent
	MAXMSG := 5 //max messages consumed

	t.Log("Making logger...")
	l, err := NewLogger("test", "localhost:4150", "")
	if err != nil {
		t.Fatal(err)
	}

	// CONSUMER //
	t.Log("Making consumer...")
	csm, err := nsq.NewConsumer("test", "test_chan", defaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Setting consumer HandlerFunc...")
	bufs := make(chan *Entry)
	csm.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		msg := new(Entry)
		err := msg.Decode(bytes.NewReader(m.Body))
		if err != nil {
			t.Fatal(err)
		}
		bufs <- msg
		return nil
	}))
	err = csm.ConnectToNSQD("localhost:4150")
	if err != nil {
		t.Fatal(err)
	}

	// WRITE MESSAGES //
	t.Log("Writing Messages...")
	//log 10 messages
	msgs := getMsgs(NMSG)
	for _, msg := range msgs {
		t.Logf("Logging message %v...", msg)
	}
	logMsgs(l, msgs)

	// COUNT MESSAGES //
	counter := 0
	t.Log("Counting received messages...")
	var msg *Entry
	for counter < MAXMSG {
		select {
		case msg = <-bufs:
			counter++
			t.Logf("Received %v", msg)
		case <-time.After(1 * time.Second):
			break
		}
	}
	if counter < NMSG {
		t.Fatalf("Sent %d messages; got %d", NMSG, counter)
	}

	// CLEANUP //
	t.Log("Cleaning up...")
	//cleanup
	csm.Stop()
	close(bufs)
	l.Close()
	t.Log("Done.")
	return
}

// benchmark end-to-end performance
func BenchmarkLogMessage(b *testing.B) {
	rand.Seed(time.Now().Unix())
	NMSG := b.N / 1000

	l, err := NewLogger("test", "localhost:4150", "")
	if err != nil {
		b.Fatal(err)
	}

	// CONSUMER //
	csm, err := nsq.NewConsumer("test", "test_chan", defaultConfig)
	if err != nil {
		b.Fatal(err)
	}
	bufs := make(chan *Entry)
	csm.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		msg := new(Entry)
		err := msg.Decode(bytes.NewReader(m.Body))
		if err != nil {
			b.Fatal(err)
		}
		bufs <- msg
		return nil
	}))
	err = csm.ConnectToNSQD("localhost:4150")
	if err != nil {
		b.Fatal(err)
	}

	// WRITE MESSAGES //
	//log 10 messages
	msgs := getMsgs(NMSG)
	b.ResetTimer()
	logMsgs(l, msgs)
	//ensure everything gets delivered

	// COUNT MESSAGES //
	counter := 0
	for counter < NMSG {
		select {
		case _ = <-bufs:
			counter++
		case <-time.After(1 * time.Second):
			break
		}
	}
	b.StopTimer()
	if counter < NMSG {
		b.Fatalf("Sent %d messages; got %d", NMSG, counter)
	}

	// CLEANUP //
	//cleanup
	csm.Stop()
	close(bufs)
	l.Close()
	return
}
