package fluxlog

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
	"math"
	"strconv"
	"unsafe"
)

type CapEntry C.Struct

func NewCapEntry(s *C.Segment) CapEntry      { return CapEntry(s.NewStruct(0, 3)) }
func NewRootCapEntry(s *C.Segment) CapEntry  { return CapEntry(s.NewRootStruct(0, 3)) }
func ReadRootCapEntry(s *C.Segment) CapEntry { return CapEntry(s.Root(0).ToStruct()) }
func (s CapEntry) Name() string              { return C.Struct(s).GetObject(0).ToText() }
func (s CapEntry) SetName(v string)          { C.Struct(s).SetObject(0, s.Segment.NewText(v)) }
func (s CapEntry) Columns() C.TextList       { return C.TextList(C.Struct(s).GetObject(1)) }
func (s CapEntry) SetColumns(v C.TextList)   { C.Struct(s).SetObject(1, C.Object(v)) }
func (s CapEntry) Points() PointsT_List      { return PointsT_List(C.Struct(s).GetObject(2)) }
func (s CapEntry) SetPoints(v PointsT_List)  { C.Struct(s).SetObject(2, C.Object(v)) }

//WriteJSON writes the influxdb-compatible form of a CapEntry
//Ex. {"name":"test_db","columns":["time","message"],"points":[103827811235,"hello!"]}
func (s CapEntry) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"name\":")
	if err != nil {
		return err
	}
	{
		s := s.Name()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"columns\":")
	if err != nil {
		return err
	}
	{
		s := s.Columns()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(",")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"points\":")
	if err != nil {
		return err
	}
	{
		s := s.Points()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(",")
				}
				if err != nil {
					return err
				}
				//This method is modified to be influxdb-compatible
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CapEntry) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}

type CapEntry_List C.PointerList

func NewCapEntryList(s *C.Segment, sz int) CapEntry_List {
	return CapEntry_List(s.NewCompositeList(0, 3, sz))
}
func (s CapEntry_List) Len() int          { return C.PointerList(s).Len() }
func (s CapEntry_List) At(i int) CapEntry { return CapEntry(C.PointerList(s).At(i).ToStruct()) }
func (s CapEntry_List) ToArray() []CapEntry {
	return *(*[]CapEntry)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type PointsT C.Struct
type PointsT_Which uint16

const (
	POINTST_INT   PointsT_Which = 0
	POINTST_FLOAT               = 1
	POINTST_TEXT                = 2
	POINTST_BOOL                = 3
)

func NewPointsT(s *C.Segment) PointsT      { return PointsT(s.NewStruct(16, 1)) }
func NewRootPointsT(s *C.Segment) PointsT  { return PointsT(s.NewRootStruct(16, 1)) }
func ReadRootPointsT(s *C.Segment) PointsT { return PointsT(s.Root(0).ToStruct()) }
func (s PointsT) Which() PointsT_Which     { return PointsT_Which(C.Struct(s).Get16(8)) }
func (s PointsT) Int() int64               { return int64(C.Struct(s).Get64(0)) }
func (s PointsT) SetInt(v int64)           { C.Struct(s).Set16(8, 0); C.Struct(s).Set64(0, uint64(v)) }
func (s PointsT) Float() float64           { return math.Float64frombits(C.Struct(s).Get64(0)) }
func (s PointsT) SetFloat(v float64) {
	C.Struct(s).Set16(8, 1)
	C.Struct(s).Set64(0, math.Float64bits(v))
}
func (s PointsT) Text() string { return C.Struct(s).GetObject(0).ToText() }
func (s PointsT) SetText(v string) {
	C.Struct(s).Set16(8, 2)
	C.Struct(s).SetObject(0, s.Segment.NewText(v))
}
func (s PointsT) Bool() bool     { return C.Struct(s).Get1(0) }
func (s PointsT) SetBool(v bool) { C.Struct(s).Set16(8, 3); C.Struct(s).Set1(0, v) }

// THIS IS MODIFIED IN ORDER TO CORRECTLY MARSHAL TO INFLUXDB-COMPATIBLE JSON
// WriteJSON uses a buffer to write the JSON-compatible serialization of PointsT
func (s PointsT) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf

	switch s.Which() {
	case POINTST_INT:
		_, err = b.Write(strconv.AppendInt([]byte{}, s.Int(), 10))
	case POINTST_FLOAT:
		_, err = b.Write(strconv.AppendFloat([]byte{}, s.Float(), 'f', 10, 64))
	case POINTST_TEXT:
		_, err = b.Write(strconv.AppendQuote([]byte{}, s.Text()))
	case POINTST_BOOL:
		_, err = b.Write(strconv.AppendBool([]byte{}, s.Bool()))
	}
	if err != nil {
		return err
	}

	err = b.Flush()
	return err
}

//WriteNoBufferJSON is identical to WriteJSON, but it doesn't use an internal buffer
//Use when 'w' is already buffered to prevent additional buffer allocations
func (s PointsT) WriteNoBufferJSON(w io.Writer) error {
	var err error

	switch s.Which() {
	case POINTST_INT:
		_, err = w.Write(strconv.AppendInt([]byte{}, s.Int(), 10))
	case POINTST_FLOAT:
		_, err = w.Write(strconv.AppendFloat([]byte{}, s.Float(), 'f', 10, 64))
	case POINTST_TEXT:
		_, err = w.Write(strconv.AppendQuote([]byte{}, s.Text()))
	case POINTST_BOOL:
		_, err = w.Write(strconv.AppendBool([]byte{}, s.Bool()))
	}

	return err
}

func (s PointsT) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}

type PointsT_List C.PointerList

func NewPointsTList(s *C.Segment, sz int) PointsT_List {
	return PointsT_List(s.NewCompositeList(16, 1, sz))
}
func (s PointsT_List) Len() int         { return C.PointerList(s).Len() }
func (s PointsT_List) At(i int) PointsT { return PointsT(C.PointerList(s).At(i).ToStruct()) }
func (s PointsT_List) ToArray() []PointsT {
	return *(*[]PointsT)(unsafe.Pointer(C.PointerList(s).ToArray()))
}
