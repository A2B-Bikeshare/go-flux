package fluxd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"reflect"
	"testing"
)

// elasticsearchdb literal
var testdb = ElasticsearchDB{
	Schema: msg.Schema{
		{Name: "name", T: msg.String},
		{Name: "age", T: msg.Int},
		{Name: "id", T: msg.Uint},
		{Name: "weight", T: msg.Float},
		{Name: "data", T: msg.Bin},
		{Name: "is_true", T: msg.Bool},
	},
	Addr:  "http://localhost:9200",
	Index: "testdb",
	Dtype: "test_type",
}

// conforms to testdb.Schema
var testdata []interface{} = make([]interface{}, 6)

func init() {
	testdata[0] = "bob"
	testdata[1] = int64(32)
	testdata[2] = uint64(10923145)
	testdata[3] = float64(150.0)
	testdata[4] = []byte{0x23, 0x47, 0x7f, 0x3c}
	testdata[5] = true
}

func TestESTranslate(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	outbuf := bytes.NewBuffer(nil)

	// write testdata to buf
	err := testdb.Schema.EncodeSlice(testdata, buf)
	if err != nil {
		t.Fatal(err)
	}

	//translate buf -> outbuf
	err = testdb.Translate(buf.Bytes(), outbuf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Encoded: %s", outbuf.String())

	dec := json.NewDecoder(outbuf)
	m := make(map[string]interface{})
	err = dec.Decode(&m)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(m["name"], testdata[0]) {
		t.Errorf("%s != %s", m["name"], testdata[0])
	}

	if !reflect.DeepEqual(m["age"], float64(testdata[1].(int64))) {
		t.Errorf("%f != %f", m["age"], float64(testdata[1].(int64)))
	}

	if !reflect.DeepEqual(m["id"], float64(testdata[2].(uint64))) {
		t.Errorf("%f != %f", m["id"], float64(testdata[2].(uint64)))
	}

	if !reflect.DeepEqual(m["weight"], testdata[3]) {
		t.Errorf("%f != %f", m["weight"], testdata[3])
	}

	databts, err := base64.StdEncoding.DecodeString(m["data"].(string))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(databts, testdata[4]) {
		t.Errorf("%v != %v", databts, testdata[4])
	}

	if !reflect.DeepEqual(m["is_true"], testdata[5]) {
		t.Errorf("%t != %t", m["is_true"], testdata[5])
	}
}
