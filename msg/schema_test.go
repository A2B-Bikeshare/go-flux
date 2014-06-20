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

func TestReadWriteSchema(t *testing.T) {
	names := []string{"float", "int", "uint", "string", "bin"}
	values := make([]interface{}, len(names))
	values[0] = float64(3.589)
	values[1] = int64(-2000)
	values[2] = uint64(586)
	values[3] = "here's a string"
	values[4] = []byte{3, 4, 5, 6, 18, 200, 100, 5}

	s, err := MakeSchema(names, values)
	if err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	s.SerializeTo(buf)

	snew, err := ReadSchema(buf)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(snew, s) {
		t.Errorf("Expected %v; got %v", s, snew)
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
	b.SetBytes(nbytes)
	buf.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Encode(values, buf)
		buf.Reset()
	}
}

func TestSchemaDecodeToSlice(t *testing.T) {
	names := []string{"float", "int", "uint", "string", "bin"}
	values := make([]interface{}, len(names))
	values[0] = float64(3.5898493027815032478)
	values[1] = int64(-2000)
	values[2] = uint64(586)
	values[3] = "here's a string"
	values[4] = []byte{3, 4, 5}

	s, err := MakeSchema(names, values)
	if err != nil {
		t.Fatal(err)
	}
	buf := bytes.NewBuffer(nil)
	buf.Grow(40)
	s.Encode(values, buf)

	outslice := make([]interface{}, len(names))

	err = s.DecodeToSlice(buf, outslice)

	for i, out := range outslice {
		//float64 does not evaluate to precisely the same value
		if _, ok := out.(float64); ok {
			continue
		}
		if !reflect.DeepEqual(values[i], out) {
			t.Errorf("Test case %d: Got %v, expected %v.", i, out, values[i])
		}
	}

}

func TestSchemaDecodetoMap(t *testing.T) {
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
	buf.Grow(40)
	s.Encode(values, buf)

	m := make(map[string]interface{})

	err = s.DecodeToMap(buf, m)
	if err != nil {
		t.Fatal(err)
	}

	for key, val := range m {
		switch key {
		case "float":
			f, ok := val.(float64)
			if !ok {
				t.Errorf("Couldn't cast %v to float64", val)
				continue
			}
			if float32(f) != float32(values[0].(float64)) {
				t.Errorf("Expected %v, got %v", values[0], f)
			}
		case "int":
			i, ok := val.(int64)
			if !ok {
				t.Errorf("Couldn't cast %v to int64", val)
				continue
			}
			if i != values[1].(int64) {
				t.Errorf("Expected %v, got %v", values[1], i)
			}
		case "uint":
			u, ok := val.(uint64)
			if !ok {
				t.Errorf("Couldn't cast %v to uint64", val)
				continue
			}
			if u != values[2].(uint64) {
				t.Errorf("Expected %v, got %v", values[2], u)
			}
		case "string":
			s, ok := val.(string)
			if !ok {
				t.Errorf("Couldn't cast %v to string", s)
				continue
			}
			if s != values[3].(string) {
				t.Errorf("Expected %s, got %s", values[3], s)
			}
		case "bin":
			bts, ok := val.([]byte)
			if !ok {
				t.Errorf("Couldn't cast %v to []byte", val)
				continue
			}
			if !reflect.DeepEqual(bts, values[4].([]byte)) {
				t.Errorf("Expected %v, got %v", values[4], bts)
			}

		default:
			t.Errorf("Unknown name in map: %q", key)
			continue
		}
	}

}

func BenchmarkSchemaDecodeToMap(b *testing.B) {
	b.ReportAllocs()
	names := []string{"float", "int", "uint", "string", "bin"}
	values := make([]interface{}, len(names))
	values[0] = float64(3.589)
	values[1] = int64(-2000)
	values[2] = uint64(586)
	values[3] = "here's a string"
	values[4] = []byte{3, 4, 5, 8}

	s, err := MakeSchema(names, values)
	if err != nil {
		b.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	buf.Grow(40)
	s.Encode(values, buf)
	b.SetBytes(int64(len(buf.Bytes())))
	bts := buf.Bytes()
	m := make(map[string]interface{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.DecodeToMap(bytes.NewReader(bts), m)
	}

}

func BenchmarkSchemaDecodeToSlice(b *testing.B) {
	b.ReportAllocs()
	names := []string{"float", "int", "uint", "string", "bin"}
	values := make([]interface{}, len(names))
	values[0] = float64(3.589)
	values[1] = int64(-2000)
	values[2] = uint64(586)
	values[3] = "here's a string"
	values[4] = []byte{3, 4, 5, 8}

	s, err := MakeSchema(names, values)
	if err != nil {
		b.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	buf.Grow(40)
	s.Encode(values, buf)
	b.SetBytes(int64(len(buf.Bytes())))
	bts := buf.Bytes()
	m := make([]interface{}, len(names))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.DecodeToSlice(bytes.NewReader(bts), m)
	}
}

func BenchmarkSchemaDecodeToSliceZeroCopy(b *testing.B) {
	b.ReportAllocs()
	names := []string{"float", "int", "uint", "string", "bin"}
	values := make([]interface{}, len(names))
	values[0] = float64(3.589)
	values[1] = int64(-2000)
	values[2] = uint64(586)
	values[3] = "here's a string"
	values[4] = []byte{3, 4, 5, 8}

	s, err := MakeSchema(names, values)
	if err != nil {
		b.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	s.Encode(values, buf)
	b.SetBytes(int64(len(buf.Bytes())))
	bts := buf.Bytes()

	m := make([]interface{}, len(names))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = s.DecodeToSliceZeroCopy(bts, m)
		if err != nil {
			b.Fatal(err)
		}
	}
}
