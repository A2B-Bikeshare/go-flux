// +build nsq

package fluxlog

import (
	//"bytes"
	"github.com/bitly/go-nsq"
	"io"
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

type TestDecoder struct{}

func (t *TestDecoder) Decode(s CapEntry, w io.Writer) error {
	return InfluxDBDecode(s, w)
}
func (t *TestDecoder) Prefix() []byte {
	return nil
}
func (t *TestDecoder) Suffix() []byte {
	return nil
}

func getMsg() Msg {
	n := rand.Intn(5)
	return Msg{LEVELS[n], MSGS[n]}
}

func getMsgs(n int) []Msg {
	out := make([]Msg, n)
	for i := 0; i < n; i++ {
		out[i] = getMsg()
	}
	return out
}

func logMsgs(l *Logger, mss []Msg) {
	for _, ms := range mss {
		l.Log(ms.Level, ms.Message)
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
	conn.Close()
	t.Log("Success.")
	time.Sleep(100 * time.Millisecond)
	return
}

func TestLogMessage(t *testing.T) {
	t.Log("Making logger...")
	l, err := NewLogger("test", "test", "localhost:4150", "")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	//consumer
	/*
		t.Log("Making consumer...")
		csm, err := nsq.NewConsumer("test", "test_chan", defaultConfig)
		if err != nil {
			t.Fatal(err)
		}
		//set consumer message handler
		t.Log("Setting consumer HandlerFunc...")
		bufs := make(chan *bytes.Buffer, 10)
		csm.SetHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
			b := bytes.NewBuffer(nil)
			err := UseDecoder(new(TestDecoder), m.Body, b)
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
	*/
	t.Log("Writing Messages...")
	//log 10 messages
	msgs := getMsgs(10)
	logMsgs(l, msgs)
	time.Sleep(1000 * time.Millisecond)
	/*
	  counter := 0

	  t.Log("Counting received messages...")
	  for counter < 10 {
	    select {
	    case buf := <-bufs:
	      counter++
	      t.Logf("Received %q", buf.String())
	    case <-time.After(1 * time.Second):
	      t.Fatal("Receive timed out.")
	      break
	    }
	  }

	  t.Log("Cleaning up...")
	  //cleanup
	  close(bufs)
	  csm.Stop()
	*/
	l.Close()
	t.Log("Done.")
	return
}
