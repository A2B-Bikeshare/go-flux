package fluxmap

import (
	"bytes"
	"encoding/binary"
	"github.com/ugorji/go/codec"
	"reflect"
	"testing"
)

var mp codec.MsgpackHandle

func assertEqual(a []byte, b []byte, t *testing.T) {
	if !reflect.DeepEqual(a, b) {
		t.Errorf("Encoded %x, but should be %x", a, b)
		t.Errorf("Used prefix %x; should be %x", a[0], b[0])
	}
	t.Logf("Encoded %x.", a)
}

func mpencode(v interface{}) (b []byte, err error) {
	dec := codec.NewEncoderBytes(&b, &mp)
	err = dec.Encode(v)
	return
}

func TestBoolWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	val := true
	writeBool(buf, val)

	mbuf, err := mpencode(val)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual(buf.Bytes(), mbuf, t)
}

func TestStringWriteFixed(t *testing.T) {
	s := "test string" //len=11 should be encoded as a fixstr
	buf := bytes.NewBuffer(nil)
	writeString(buf, s)

	mbuf, err := mpencode(s)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual(buf.Bytes(), mbuf, t)
}

func TestStringWrite8(t *testing.T) {
	//  We're skipping this test for now, because the codec package doesn't correctly
	// identify this string as a 'str8'-compatible string (it thinks this is a str16 string)
	t.Skip()
	s := "Pickled cliche stumptown, swag ethnic authentic drinking vinegar. Sustainable butcher crucifix, Marfa vegan Pinterest skateboard four loko McSweeney's fap iPhone wolf twee. vegan four loko hashtag kogi. Wayfarers Vice McSweeny's,"
	t.Logf("Using string length %d", len(s))
	if len(s) > 256 || len(s) < 32 {
		t.Fatal("Invalid string length. Does not target the 8-bit length case.")
	}

	buf := bytes.NewBuffer(nil)
	writeString(buf, s)

	mbuf, err := mpencode(s)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual(buf.Bytes(), mbuf, t)
}

func TestStringWrite16(t *testing.T) {
	s := "Swag put a bird on it lo-fi beard. VHS iPhone ethnic meggings chillwave 90's lo-fi. Fixie XOXO VHS, Austin ugh art party keffiyeh asymmetrical drinking vinegar salvia fashion axe readymade retro. Ethical brunch Wes Anderson Shoreditch normcore locavore distillery pork belly. Salvia single-origin coffee kogi tote bag, iPhone craft beer wolf chambray letterpress Echo Park. Hashtag Blue Bottle fixie tousled, church-key Austin ethical mustache keffiyeh freegan hella ennui put a bird on it. Flannel Truffaut fixie shabby chic, Marfa craft beer McSweeney's semiotics skateboard Wes Anderson retro chillwave deep v."
	t.Logf("Using string length %d", len(s))
	if len(s) < 256 || len(s) > (1<<32-1) {
		t.Fatal("Invalid string length. Does not target the 16-bit length case.")
	}

	buf := bytes.NewBuffer(nil)
	writeString(buf, s)

	mbuf, err := mpencode(s)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual(buf.Bytes(), mbuf, t)
}

func TestPosIntWrite(t *testing.T) {
	bigend := binary.BigEndian

	//positive integers
	var fix int64 = 50            //fixint
	var small int64 = 150         //int8
	var med int64 = 1<<15 - 30    //int16
	var large int64 = 1<<31 - 400 //int32
	var huge int64 = 1 << 40      //int64
	buf := bytes.NewBuffer(nil)
	var err error
	var prefix byte

	//fixint case
	writeInt(buf, fix)
	t.Log("fixint case...")
	if prefix, _ = buf.ReadByte(); prefix > mfixintMAX {
		t.Errorf("Used prefix %x, should be %x to %x", prefix, mfixint, mfixintMAX)
	}
	err = buf.UnreadByte() //fixints are encoded in 1 byte
	if err != nil {
		t.Fatal(err)
	}
	testfix := int8(0)
	err = binary.Read(buf, bigend, &testfix)
	if err != nil {
		t.Fatal(err)
	}
	if testfix != int8(fix) {
		t.Errorf("Expected return value %d; got %d", fix, testfix)
	}
	buf.Reset()

	//int8 case
	writeInt(buf, small)
	t.Log("int8 case...")
	if prefix, _ = buf.ReadByte(); prefix != mint8 {
		t.Errorf("Used prefix %x, should be %x", prefix, mint8)
	}
	testsmall := int8(0)
	err = binary.Read(buf, bigend, &testsmall)
	if err != nil {
		t.Fatal(err)
	}
	if testsmall != int8(small) {
		t.Errorf("Expected return value %d; got %d", small, testsmall)
	}
	buf.Reset()

	//int16 case
	writeInt(buf, med)
	t.Log("int16 case...")
	if prefix, _ = buf.ReadByte(); prefix != mint16 {
		t.Errorf("Used prefix %x, should be %x", prefix, mint16)
	}
	testmed := int16(0)
	err = binary.Read(buf, bigend, &testmed)
	if err != nil {
		t.Fatal(err)
	}
	if testmed != int16(med) {
		t.Errorf("Expected return value %d; got %d", med, testmed)
	}
	buf.Reset()

	//int32 case
	writeInt(buf, large)
	t.Log("int32 case...")
	if prefix, _ = buf.ReadByte(); prefix != mint32 {
		t.Errorf("Used prefix %x, should be %x", prefix, mint32)
	}
	testlarge := int32(0)
	err = binary.Read(buf, bigend, &testlarge)
	if err != nil {
		t.Fatal(err)
	}
	if testlarge != int32(large) {
		t.Errorf("Expected return value %d; got %d", large, testlarge)
	}
	buf.Reset()

	//int64 case
	writeInt(buf, huge)
	t.Log("int64 case...")
	if prefix, _ = buf.ReadByte(); prefix != mint64 {
		t.Errorf("Used prefix %x, should be %x", prefix, mint64)
	}
	testhuge := int64(0)
	err = binary.Read(buf, bigend, &testhuge)
	if err != nil {
		t.Fatal(err)
	}
	if testhuge != int64(huge) {
		t.Errorf("Expected return value %d; got %d", huge, testhuge)
	}
	buf.Reset()
}
