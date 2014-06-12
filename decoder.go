package fluxlog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"io"
)

var (
	//ErrCapEntryMalformed is returned when the lengths of 'points' and 'columns' are unequal
	ErrCapEntryMalformed = errors.New("Malformed CapEntry.")
)

//Decoder must be satisfied for each database type
type Decoder interface {
	Decode(s CapEntry, w io.Writer) error
	Prefix() []byte
	Suffix() []byte
}

//ElasticSearchDecode writes 's' into 'w'
func ElasticSearchDecode(s CapEntry, w io.Writer) error {
	return s.WriteESJSON(w)
}

//InfluxDBDecode writes 's' into 'w'
func InfluxDBDecode(s CapEntry, w io.Writer) error {
	return s.WriteJSON(w)
}

//UseDecoder uses Decoder 'dfunc' to write one or more CapEntry types to
//a buffer UseDecoder can read multiple emails. 'd' is allowed to be nil; a new buffer will be allocated for you.
//Data is appended to 'd'
func UseDecoder(d Decoder, data []byte, b *bytes.Buffer) (err error) {
	if d == nil {
		b = getBuffer()
	}

	var seg *capn.Segment
	var n int64
	for seg, n, err = capn.ReadFromMemoryZeroCopy(data); err != io.EOF; data = data[n:] {
		if err != nil {
			return
		}
		entry := ReadRootCapEntry(seg)
		_, err = b.Write(d.Prefix())
		if err != nil {
			return
		}
		err = d.Decode(entry, b)
		if err != nil {
			return
		}
		_, err = b.Write(d.Suffix())
		if err != nil {
			return
		}
	}
	return
}

//WriteESJSON writes elasticsearch-compatible JSON for the _bulk API
func (s CapEntry) WriteESJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	err = b.WriteByte('{')
	if err != nil {
		return err
	}

	//Write "name:"
	_, err = b.WriteString("\"name\":")
	if err != nil {
		return err
	}

	//Write "[name]"
	_, err = b.WriteString(fmt.Sprintf("%q,", s.Name()))
	if err != nil {
		return err
	}

	//Write all columns/points
	pts := s.Points().ToArray()
	cols := s.Columns().ToArray()
	if len(pts) != len(cols) {
		return ErrCapEntryMalformed
	}
	for i, colname := range cols {
		_, err = b.WriteString(fmt.Sprintf("%q:", colname))
		if err != nil {
			return err
		}

		err = pts[i].WriteNoBufferJSON(b)
		if err != nil {
			return err
		}
		if i < len(cols)-1 {
			err = b.WriteByte(',')
			if err != nil {
				return err
			}
		}
	}

	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
