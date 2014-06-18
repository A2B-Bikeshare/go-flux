package log

import (
	"bytes"
	"encoding/json"
	capn "github.com/glycerine/go-capnproto"
	"github.com/philhofer/gringo"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
)

const (
	testName    = "testName"
	testLevel   = WARN
	testMessage = "testMessage here!"
)

type TestDecoder struct{}

func (t TestDecoder) Decode(s CapEntry, w io.Writer) error {
	return InfluxDBDecode(s, w)
}
func (t TestDecoder) Prefix() []byte {
	return nil
}
func (t TestDecoder) Suffix() []byte {
	return nil
}

func TestBufferPool(t *testing.T) {
	bufs := make([]*bytes.Buffer, 0, 10)
	for i := 0; i < 1000; i++ {
		//get [0,10) buffers
		nOut := rand.Intn(10)
		for j := 0; j < nOut; j++ {
			bufs = append(bufs, getBuffer())
		}

		//put [0,10) buffers
		for k := 0; k < nOut; k++ {
			tbuf := bufs[nOut-k-1]
			putBuffer(tbuf)
		}
		//reset
		bufs = bufs[0:]
	}
}

func MakeLogMsg() *capn.Segment {
	seg := capn.NewBuffer(getBytes())
	LogMsgtoSegment(seg, testName, testLevel, testMessage)
	return seg
}

// Test that Gringo can handle aggressive concurrent
// reads and writes without failure
func TestFloodGringo(t *testing.T) {
	NUMMSG := 10000
	msgs := make([]*capn.Segment, NUMMSG)
	for i := 0; i < NUMMSG; i++ {
		msgs[i] = MakeLogMsg()
	}
	outmsgs := make([]*capn.Segment, NUMMSG)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	g := gringo.NewGringo()
	//start writer
	go func() {
		for _, msg := range msgs {
			g.Write(msg)
		}
		wg.Done()
	}()
	//start reader
	go func() {
		for i := 0; i < NUMMSG; i++ {
			outmsgs[i] = g.Read()
		}
		wg.Done()
	}()
	wg.Wait()
	//test for equality and correct ordering
	for i, msg := range outmsgs {
		if msg != msgs[i] {
			t.Fatalf("Arrays unequal at index %d; %x != %x", i, msg, msgs[i])
		}
	}

}

//Encoding Speed
func BenchmarkLogtoSeg(b *testing.B) {
	b.SetBytes(280)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		bt := getBytes()
		seg := capn.NewBuffer(bt)
		b.StartTimer()
		LogMsgtoSegment(seg, testName, testLevel, testMessage)
		b.StopTimer()
		seg = nil
		b.StartTimer()
		putBytes(bt)
	}
}

//Encoding+Decoding Speed for Elasticsearch output
func BenchmarkElasticsearchEndtoEnd(b *testing.B) {
	b.ReportAllocs()
	var err error
	buf := getBuffer()
	inseg := MakeLogMsg()
	n, err := inseg.WriteTo(buf)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(n)
	out := ioutil.Discard
	var seg *capn.Segment

	for i := 0; i < b.N; i++ {
		seg, _, err = capn.ReadFromMemoryZeroCopy(buf.Bytes())
		if err != nil {
			b.Fatal(err)
		}
		ReadRootCapEntry(seg).WriteESJSON(out)

		//re-write to buffer
		buf.Reset()
		_, err = MakeLogMsg().WriteTo(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	putBuffer(buf)
}

//Encoding+Decoding speed for InfluxDB output
func BenchmarkInfluxDBEndtoEnd(b *testing.B) {
	b.ReportAllocs()
	var err error
	buf := getBuffer()
	inseg := MakeLogMsg()
	n, err := inseg.WriteTo(buf)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(n)
	out := ioutil.Discard
	var seg *capn.Segment

	for i := 0; i < b.N; i++ {
		seg, _, err = capn.ReadFromMemoryZeroCopy(buf.Bytes())
		if err != nil {
			b.Fatal(err)
		}
		ReadRootCapEntry(seg).WriteJSON(out)

		//re-write to buffer
		buf.Reset()
		_, err = MakeLogMsg().WriteTo(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	putBuffer(buf)
}

func TestMakeCapLog(t *testing.T) {
	seg := MakeLogMsg()
	entry := ReadRootCapEntry(seg)

	if entry.Name() != testName {
		t.Fatalf("Expected Name()=%q, got %q", testName, entry.Name())
	}

	cols := entry.Columns()
	if cols.Len() != 3 {
		t.Fatalf("Expected 3 column names, got %d", cols.Len())
	}
	if cols.At(0) != "time" {
		t.Fatalf("Expected %q as first column name, found %q", "time", cols.At(0))
	}
	if cols.At(1) != "level" {
		t.Fatalf("Expected %q as second column name, found %q", "level", cols.At(1))
	}
	if cols.At(2) != "message" {
		t.Fatalf("Expected %q as third column name, found %q", "message", cols.At(2))
	}

	points := entry.Points()
	if points.Len() != 3 {
		t.Fatalf("Expected 3 points, got %d", points.Len())
	}
	pointSlice := entry.Points().ToArray()
	if pointSlice[0].Which() != POINTST_INT {
		t.Fatalf("Expected first point 'which' to be 0, got %d", pointSlice[0].Which())
	}
	t.Logf("Got timetsamp: %d", pointSlice[0].Int())
	if pointSlice[1].Which() != POINTST_INT {
		t.Fatalf("Expected second point 'which' to be 0, got %d", pointSlice[1].Which())
	}
	if pointSlice[1].Int() != int64(testLevel) {
		t.Fatalf("Expected second point to be 0, got %d", pointSlice[1].Int())
	}
	if pointSlice[2].Which() != POINTST_TEXT {
		t.Fatalf("Expected third point 'which' to be 2, got %d", pointSlice[2].Which())
	}
	if pointSlice[2].Text() != testMessage {
		t.Fatalf("Expected message %q, got %q", testMessage, pointSlice[2].Text())
	}
	return
}

func TestUseDecoder(t *testing.T) {
	seg := MakeLogMsg()
	nbuf := getBuffer()
	n, err := seg.WriteTo(nbuf)
	if err != nil || n == 0 {
		t.Fatal(err)
	}
	dat := nbuf.Bytes()
	buf := getBuffer()
	//write contents of nbuf.Bytes() ([]byte) to buf as JSON
	err = UseDecoder(TestDecoder{}, dat, buf)
	if err != nil {
		t.Fatal(err)
	}

	smp := make(map[string]interface{})
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	t.Log(buf.String())
	err = dec.Decode(&smp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Unmarshaled element: #%v", smp)

	//TEST NAME
	name, ok := smp["name"]
	if !ok {
		t.Fatal("'name' not found in unmarshalled map")
	}
	if name != testName {
		t.Fatalf("Expected %q for 'name', got %q", testName, name)
	}

	//TEST COLUMNS
	cols, ok := smp["columns"]
	if !ok {
		t.Fatal("'columns' not found in unmarshalled map")
	}
	colslice, ok := cols.([]interface{})
	if !ok {
		t.Fatal("'columns' could not be cast to []interface{}")
	}
	if len(colslice) != 3 {
		t.Fatal("'columns' not the right length")
	}
	if colslice[0].(string) != "time" {
		t.Fatalf("Expected 'time', got %q", colslice[0])
	}
	if colslice[1].(string) != "level" {
		t.Fatalf("Expected 'level', got %q", colslice[1])
	}
	if colslice[2].(string) != "message" {
		t.Fatalf("Expected 'message', got %q", colslice[2])
	}

	//TEST POINTS
	pts, ok := smp["points"]
	if !ok {
		t.Fatal("'points' not found in unmarshalled map")
	}
	ptslice, ok := pts.([]interface{})
	if !ok {
		t.Fatal("'points' could not be cast to []interface{}")
	}
	//check lengths
	if len(ptslice) != 1 {
		t.Fatal("Length of [][]interface{} is not 1")
	}
	ptslice, ok = ptslice[0].([]interface{})
	//check length again
	if len(ptslice) != 3 {
		t.Fatal("Length of []interface{} is not 3")
	}

	if time, err := ptslice[0].(json.Number).Int64(); err == nil {
		t.Logf("Got timestamp %d", time)
	} else {
		t.Fatal("First 'points' element could not be cast to int64")
	}
	if level, err := ptslice[1].(json.Number).Int64(); err == nil {
		if level != int64(testLevel) {
			t.Fatalf("Expected level %d, got level %d", testLevel, level)
		}
	} else {
		t.Fatal("Second 'points' element could not be cast to int64")
	}
	if msg, ok := ptslice[2].(string); ok {
		if msg != testMessage {
			t.Fatalf("Expected message %q, got message %q", testMessage, msg)
		}
	} else {
		t.Fatal("Third 'points' element could not be cast to string")
	}
	putBuffer(buf)
	putBuffer(nbuf)
	return
}

func TestCapLogMarshal(t *testing.T) {
	seg := MakeLogMsg()
	buf := getBuffer()
	entry := ReadRootCapEntry(seg)
	smp := make(map[string]interface{})
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	err := entry.WriteJSON(buf)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(buf.String())
	err = dec.Decode(&smp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Unmarshaled element: #%v", smp)

	//TEST NAME
	name, ok := smp["name"]
	if !ok {
		t.Fatal("'name' not found in unmarshalled map")
	}
	if name != testName {
		t.Fatalf("Expected %q for 'name', got %q", testName, name)
	}

	//TEST COLUMNS
	cols, ok := smp["columns"]
	if !ok {
		t.Fatal("'columns' not found in unmarshalled map")
	}
	colslice, ok := cols.([]interface{})
	if !ok {
		t.Fatal("'columns' could not be cast to []interface{}")
	}
	if len(colslice) != 3 {
		t.Fatal("'columns' not the right length")
	}
	if colslice[0].(string) != "time" {
		t.Fatalf("Expected 'time', got %q", colslice[0])
	}
	if colslice[1].(string) != "level" {
		t.Fatalf("Expected 'level', got %q", colslice[1])
	}
	if colslice[2].(string) != "message" {
		t.Fatalf("Expected 'message', got %q", colslice[2])
	}

	//TEST POINTS
	pts, ok := smp["points"]
	if !ok {
		t.Fatal("'points' not found in unmarshalled map")
	}
	ptslice, ok := pts.([]interface{})
	if !ok {
		t.Fatal("'points' could not be cast to []interface{}")
	}
	//check lengths
	if len(ptslice) != 1 {
		t.Fatal("Length of [][]interface{} is not 1")
	}
	ptslice, ok = ptslice[0].([]interface{})
	//check length again
	if len(ptslice) != 3 {
		t.Fatal("Length of []interface{} is not 3")
	}

	if time, err := ptslice[0].(json.Number).Int64(); err == nil {
		t.Logf("Got timestamp %d", time)
	} else {
		t.Fatal("First 'points' element could not be cast to int64")
	}
	if level, err := ptslice[1].(json.Number).Int64(); err == nil {
		if level != int64(testLevel) {
			t.Fatalf("Expected level %d, got level %d", testLevel, level)
		}
	} else {
		t.Fatal("Second 'points' element could not be cast to int64")
	}
	if msg, ok := ptslice[2].(string); ok {
		if msg != testMessage {
			t.Fatalf("Expected message %q, got message %q", testMessage, msg)
		}
	} else {
		t.Fatal("Third 'points' element could not be cast to string")
	}
	putBuffer(buf)
	return
}

func TestCapLogMarshalElasticSearch(t *testing.T) {
	seg := MakeLogMsg()
	buf := getBuffer()
	entry := ReadRootCapEntry(seg)
	smp := make(map[string]interface{})
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	err := entry.WriteESJSON(buf)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(buf.String())
	err = dec.Decode(&smp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Unmarshaled element: #%v", smp)

	for key, val := range smp {
		switch key {
		case "name":
			name, ok := val.(string)
			if !ok {
				t.Fatal("Couldn't cast 'name' value to string")
			}
			if name != testName {
				t.Fatalf("Expected %q, got %q", testName, name)
			}
		case "time":
			time, err := val.(json.Number).Int64()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Got timestamp %d", time)
		case "level":
			if level, err := val.(json.Number).Int64(); err == nil {
				if level != int64(testLevel) {
					t.Fatalf("Expected %d, got %d", testLevel, level)
				}
			} else {
				t.Fatal("Couldn't cast 'level' to int64")
			}
		case "message":
			if msg, ok := val.(string); ok {
				if msg != testMessage {
					t.Fatalf("Expected %q, got %q", testMessage, msg)
				}
			} else {
				t.Fatal("Couldn't cast 'msg' to string")
			}
		default:
			t.Fatalf("Unkown element %q in unmarshalled map", key)
		}
	}
	putBuffer(buf)
	return
}
