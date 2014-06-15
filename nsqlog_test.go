// +build nsq

package fluxlog

import (
	//"bytes"
	"bytes"
	"github.com/bitly/go-nsq"
	"math/rand"
	"testing"
	"time"
)

var (
	MSGS   = []string{"This is debug info.", "This is info info.", "This is a warning.", "This is an error...", "This is FATALLLL"}
	LEVELS = []LogLevel{0, 1, 2, 3, 4}
)

type Msg struct {
	Level   LogLevel
	Message string
}

func getMsg() Msg {
	n := rand.Intn(5)
	return Msg{Level: LEVELS[n], Message: MSGS[n]}
}

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
		l.Log(msg.Level, msg.Message)
	}
}

func TestConnection(t *testing.T) {
	t.Log("Testing connection...")
	conn := nsq.NewConn("localhost:4150", defaultConfig)
	id, err := conn.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(id)
	time.Sleep(500 * time.Millisecond)
	conn.Close()
	t.Log("Success.")
	return
}

func TestLogMessage(t *testing.T) {
	t.Log("Making logger...")
	l, err := NewLogger("test", "test", "localhost:4150", "")
	if err != nil {
		t.Fatal(err)
	}
	//wait for setup
	time.Sleep(50 * time.Millisecond)

	// CONSUMER //
	t.Log("Making consumer...")
	csm, err := nsq.NewConsumer("test", "test_chan", defaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Setting consumer HandlerFunc...")
	bufs := make(chan *bytes.Buffer, 10)
	csm.SetHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		b := getBuffer()
		dat := m.Body
		err := UseDecoder(TestDecoder{}, dat, b)
		if err != nil {
			t.Fatal(err)
		}
		bufs <- b
		return nil
	}))
	err = csm.ConnectToNSQD("localhost:4150")
	if err != nil {
		t.Fatal(err)
	}

	// WRITE MESSAGES //
	t.Log("Writing Messages...")
	//log 10 messages
	msgs := getMsgs(10)
	for _, msg := range msgs {
		t.Logf("Logging message %v...", msg)
	}
	logMsgs(l, msgs)
	time.Sleep(1000 * time.Millisecond)

	//count messages
	counter := 0
	t.Log("Counting received messages...")
	for counter < 10 {
		select {
		case buf := <-bufs:
			counter++
			t.Logf("Received %q", buf.String())
			putBuffer(buf)
		case <-time.After(5 * time.Second):
			t.Fatal("Receive timed out.")
			break
		}
	}

	t.Log("Cleaning up...")
	//cleanup
	csm.Stop()
	time.Sleep(500 * time.Millisecond)
	close(bufs)
	l.Close()
	t.Log("Done.")
	return
}
