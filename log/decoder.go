package log

import (
	"bufio"
	"bytes"
	"errors"
	capn "github.com/glycerine/go-capnproto"
	"io"
	"strconv"
)

var (
	//ErrCapEntryMalformed is returned when the lengths of 'points' and 'columns' are unequal
	ErrCapEntryMalformed = errors.New("Malformed CapEntry.")
)

//Decoder must be satisfied for each database type
//UseDecoder takes a decoder and writes a prefix, a body with 'Decode()', and then a suffix
type Decoder interface {
	//Decode marshals some form of 's' into 'w'
	Decode(s CapEntry, w io.Writer) error
	//Prefix goes before an entry, e.g. '[' (for InfluxDB)
	Prefix() []byte
	//Suffix follows an entry, e.g. ']' (for InfluxDB)
	Suffix() []byte
}

// ElasticSearchDecode writes the Elasticsearch-compatible
// serialized JSON form of a CapEntry into a writer
func ElasticSearchDecode(s CapEntry, w io.Writer) error {
	return s.WriteESJSON(w)
}

// InfluxDBDecode writes the InfluxDB-compatible
// serialized JSON form of a CapEntry into a writer
func InfluxDBDecode(s CapEntry, w io.Writer) error {
	return s.WriteJSON(w)
}

// UseDecoder uses a Decoder to write a bytewise Capnproto entry
// to a buffer.
func UseDecoder(d Decoder, data []byte, b *bytes.Buffer) (err error) {
	if b == nil {
		b = getBuffer()
	}

	var seg *capn.Segment
	seg, _, err = capn.ReadFromMemoryZeroCopy(data)
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
	_, err = b.Write(strconv.AppendQuote([]byte{}, s.Name()))
	if err != nil {
		return err
	}
	err = b.WriteByte(',')
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
		_, err = b.Write(strconv.AppendQuote([]byte{}, colname))
		if err != nil {
			return err
		}
		err = b.WriteByte(':')
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
