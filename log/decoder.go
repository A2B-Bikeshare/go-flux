package log

import (
	"github.com/A2B-Bikeshare/go-flux/msg"
	"time"
)

// Entry is a simple timestamped leveled message.
// It satisfies the msg.StructMessage interface.
type Entry struct {
	stamp   uint64
	Level   int64
	Message string
}

// Timestamp returns the timestamp of an entry,
// or time.Now() if it hasn't been stamped yet.
func (e *Entry) Timestamp() uint64 {
	if e.stamp == 0 {
		return uint64(time.Now().Unix())
	}
	return e.stamp
}

// Stamp fixes the timestamp on an entry to the moment
// Stamp() is called.
func (e *Entry) Stamp() {
	e.stamp = uint64(time.Now().Unix())
}

// Encode writes an entry with a timestamp of time.Now().Unix()
func (e *Entry) Encode(w msg.Writer) error {
	msg.WriteUint(w, e.Timestamp())
	msg.WriteInt(w, e.Level)
	msg.WriteString(w, e.Message)
	return nil
}

// Decode reads an Entry from a msg.Reader
func (e *Entry) Decode(r msg.Reader) error {
	stamp, err := msg.ReadUint(r)
	if err != nil {
		return err
	}
	level, err := msg.ReadInt(r)
	if err != nil {
		return err
	}
	msg, err := msg.ReadString(r)
	if err != nil {
		return err
	}
	e.stamp, e.Level, e.Message = stamp, level, msg
	return nil
}
