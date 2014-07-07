package fluxd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"reflect"
	"testing"
)

// Each entry should be json.Marshal-able to this struct type
type Influx struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

// elasticsearchdb literal
var testInfluxdb = InfluxDB{
	Schema: msg.Schema{
		{Name: "name", T: msg.String},
		{Name: "age", T: msg.Int},
		{Name: "id", T: msg.Uint},
		{Name: "weight", T: msg.Float},
		{Name: "data", T: msg.Bin},
		{Name: "is_true", T: msg.Bool},
	},
	Addr:   "http://localhost:8086",
	DBname: "testdb",
}

func validate(d *Influx, t *testing.T) {
	if len(d.Columns) != len(d.Points[0]) {
		t.Errorf("There are %d columns and %d points", len(d.Columns), len(d.Points[0]))
	}
	if len(d.Points) > 1 {
		t.Errorf("There are %d points arrays; there should only be 1", len(d.Points))
	}
}

// assert len(ds) == n
func batchValidate(ds InfluxEntries, n int, t *testing.T) {
	if len(ds) != n {
		t.Errorf("Expected %d entries; found %d", n, len(ds))
	}
	for i, d := range ds {
		if len(d.Columns) != len(d.Points[0]) {
			t.Errorf("Entry %d: There are %d columns and %d points", i, len(d.Columns), len(d.Points[0]))
		}
		if len(d.Points) > 1 {
			t.Errorf("Entry %d: There are %d points arrays; there should only be 1", i, len(d.Points))
		}
	}
}

// A collection of entries should be json.Marshal-able to this type
type InfluxEntries []*Influx

func TestSingleInfluxTranslate(t *testing.T) {
	testbuf := bytes.NewBuffer(nil)
	err := testInfluxdb.Schema.EncodeSlice(testdata, testbuf)
	if err != nil {
		t.Fatal(err)
	}
	outbuf := bytes.NewBuffer(nil)
	err = testInfluxdb.Translate(testbuf.Bytes(), outbuf)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(outbuf)
	t.Logf("Buffer string: %q", outbuf.String())
	ifl := new(Influx)
	err = dec.Decode(ifl)
	if err != nil {
		t.Fatal(err)
	}
	validate(ifl, t)
	if ifl.Name != testdata[0].(string) {
		t.Errorf("Decoded name is %q; should be %q", ifl.Name, testdata[0].(string))
	}
	if !reflect.DeepEqual(ifl.Columns, []string{"age", "id", "weight", "data", "is_true"}) {
		t.Errorf("Decoded columns as %v", ifl.Columns)
	}
	// we need to type-assert to float64 b/c of JSON
	if ifl.Points[0][0].(float64) != float64(testdata[1].(int64)) {
		t.Errorf("Encoded %d as %d", testdata[1].(int64), ifl.Points[0][0].(int64))
	}
	if ifl.Points[0][1].(float64) != float64(testdata[2].(uint64)) {
		t.Errorf("Encoded %d as %d", testdata[2].(uint64), ifl.Points[0][1].(uint64))
	}
	if ifl.Points[0][3] != base64.StdEncoding.EncodeToString(testdata[4].([]byte)) {
		t.Errorf("Binary encoded as %q; should be %q", ifl.Points[0][3], base64.StdEncoding.EncodeToString(testdata[4].([]byte)))
	}
	if ifl.Points[0][4] != testdata[5].(bool) {
		t.Errorf("Bool encoded as %t; should be %t", ifl.Points[0][4], testdata[5])
	}
}

func TestBatchInfluxTranslate(t *testing.T) {
	testbuf := bytes.NewBuffer(nil)
	err := testInfluxdb.Schema.EncodeSlice(testdata, testbuf)
	if err != nil {
		t.Fatal(err)
	}

	outbuf := bytes.NewBuffer(nil)
	outbuf.Write(testInfluxdb.BatchPrefix())
	for i := 0; i < 10; i++ {
		if i != 0 {
			outbuf.Write(testInfluxdb.Concat())
		}
		outbuf.Write(testInfluxdb.EntryPrefix())
		err := testInfluxdb.Translate(testbuf.Bytes(), outbuf)
		if err != nil {
			t.Fatal(err)
		}
		outbuf.Write(testInfluxdb.EntryPostfix())
	}
	outbuf.Write(testInfluxdb.BatchPostfix())

	var ifls InfluxEntries
	dec := json.NewDecoder(outbuf)
	err = dec.Decode(&ifls)
	if err != nil {
		t.Fatalf("Decode error: %s", err.Error())
	}

	if len(ifls) != 10 {
		t.Errorf("Expected length 10, got %d", len(ifls))
	}
	for _, d := range ifls {
		validate(d, t)
	}
}
