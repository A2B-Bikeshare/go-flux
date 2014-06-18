package msg

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMakeSchema(t *testing.T) {
	names := []string{"float", "int", "uint", "string", "bin"}
	values := make([]interface{}, len(names))
	values[0] = float64(3.589)
	values[1] = int64(-2000)
	values[2] = uint64(586)
	values[3] = "here's a string"
	values[4] = []byte{3, 4, 5}

	s, err := MakeSchema(names, values)
	if err != nil {
		t.Fatal(err)
	}

	for i, o := range *s {
		if !reflect.DeepEqual(names[i], o.Name) {
			t.Errorf("Test case %d: Expected name %q, got %q", i, names[i], o.Name)
		}

		switch o.T {
		case Float:
			f, ok := values[i].(float64)
			if !ok {
				t.Errorf("Test case %d: Couldn't marshal to type %v", i, Float)
			}
			if !reflect.DeepEqual(f, values[i]) {
				t.Errorf("%v != %v", f, values[i])
			}
		case Int:
			s, ok := values[i].(int64)
			if !ok {
				t.Errorf("Test case %d: Couldn't marshal to type %v", i, Int)
			}
			if !reflect.DeepEqual(s, values[i]) {
				t.Errorf("%v != %v", s, values[i])
			}
		case Uint:
			u, ok := values[i].(uint64)
			if !ok {
				t.Errorf("Test case %d: Couldn't marshal to type %v", i, Uint)
			}
			if !reflect.DeepEqual(u, values[i]) {
				t.Errorf("%v != %v", u, values[i])
			}
		case Bin:
			b, ok := values[i].([]byte)
			if !ok {
				t.Errorf("Test case %d: Couldn't marshal to type %v", i, Bin)
			}
			if !reflect.DeepEqual(b, values[i]) {
				t.Errorf("%v != %v", b, values[i])
			}
		case String:
			str, ok := values[i].(string)
			if !ok {
				t.Errorf("Test case %d: Couldn't marshal to type %v", i, String)
			}
			if !reflect.DeepEqual(str, values[i]) {
				t.Errorf("%q != %q", str, values[i])
			}
		default:
			t.Errorf("Test case %d: Unrecognized type %d", i, o.T)
		}
	}
}

func TestEncode(t *testing.T) {
	names := []string{"float", "int", "uint", "string", "bin"}
	values := make([]interface{}, len(names))
	values[0] = float64(3.589)
	values[1] = int64(-2000)
	values[2] = uint64(586)
	values[3] = "here's a string"
	values[4] = []byte{3, 4, 5}

	s, err := MakeSchema(names, values)
	if err != nil {
		t.Fatal(err)
	}
	buf := bytes.NewBuffer(nil)
	vbuf := bytes.NewBuffer(nil)
	buf.Grow(40)
	vbuf.Grow(40)

	//use encoder
	err = s.Encode(values, buf)
	if err != nil {
		t.Fatal(err)
	}

	//encode manually
	writeFloat(vbuf, values[0].(float64))
	writeInt(vbuf, values[1].(int64))
	writeUint(vbuf, values[2].(uint64))
	writeString(vbuf, values[3].(string))
	writeBin(vbuf, values[4].([]byte))

	if !reflect.DeepEqual(buf, vbuf) {
		t.Fatal("Buffers are not equal.")
	}

}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	names := []string{"float", "int", "uint", "string", "bin"}
	values := make([]interface{}, len(names))
	values[0] = float64(3.589)
	values[1] = int64(-2000)
	values[2] = uint64(586)
	values[3] = "here's a string"
	values[4] = []byte{3, 4, 5}

	s, err := MakeSchema(names, values)
	if err != nil {
		b.Fatal(err)
	}
	buf := bytes.NewBuffer(nil)
	buf.Grow(40)
	s.Encode(values, buf)
	nbytes := int64(len(buf.Bytes()))
	b.Logf("Data is %d bytes after encoding.", nbytes)
	b.SetBytes(nbytes)
	buf.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Encode(values, buf)
		buf.Reset()
	}
}
