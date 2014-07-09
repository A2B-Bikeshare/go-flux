package main

import (
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/log"
	"github.com/A2B-Bikeshare/go-flux/msg"
)

// This is the message type we are receiving
type Tele struct {
	Name string
	Dir  string
	Val  float64
	Uid  uint64
	Chrg int64
}

// Encode fulfills the msg.Encoder interface
func (t *Tele) Encode(w msg.Writer) error {
	msg.WriteString(w, t.Name)
	msg.WriteString(w, t.Dir)
	msg.WriteFloat64(w, t.Val)
	msg.WriteUint(w, t.Uid)
	msg.WriteInt(w, t.Chrg)
	return nil
}

func main() {
	fluxl, err := log.NewLogger("demotopic", ":4150", "")
	if err != nil {
		panic(err)
	}

	newtele := &Tele{"ERROR", "/bin", 1.388, 67890, -1}
	fluxl.Send(newtele)
	fluxl.Close()
	fmt.Println("Message sent.")
}
