package msg

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func TestReadExtZeroCopy(t *testing.T) {
	testbytes := []byte{1, 2, 3, 4}
	var etype int8 = 4

	buf := bytes.NewBuffer(nil)
	writeExt(buf, etype, testbytes)

	dat, netype, n, err := readExtZeroCopy(buf.Bytes())

	if err != nil {
		t.Fatal(err)
	}

	if n != len(buf.Bytes()) {
		t.Errorf("Read %d bytes; should have read %d", n, len(buf.Bytes()))
	}

	if netype != etype {
		t.Errorf("Type %d != type %d", etype, netype)
	}

	if !reflect.DeepEqual(testbytes, dat) {
		t.Errorf("Bytes %v != bytes %v", testbytes, dat)
	}

}

func TestReadBinZeroCopy(t *testing.T) {
	testbytes := []byte{1, 8, 3, 48, 201, 191, 3, 9}
	buf := bytes.NewBuffer(nil)
	writeBin(buf, testbytes)

	dat, n, err := readBinZeroCopy(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if n != len(buf.Bytes()) {
		t.Errorf("Supposed to read %d bytes; got %d bytes", len(buf.Bytes()), n)
	}

	if !reflect.DeepEqual(dat, testbytes) {
		t.Errorf("Bytes not equal: %v != %v", dat, testbytes)
	}

}

func TestReadBool(t *testing.T) {
	//test cases
	testvals := []bool{false, true}
	testbytes := []byte{mfalse, mtrue}

	buf := bytes.NewBuffer(testbytes)
	for _, b := range testvals {
		val, err := readBool(buf)
		if err != nil {
			t.Fatal(err)
		}
		if val != b {
			t.Fatalf("Got %t, expected %t", val, b)
		}
	}
}

func TestReadBoolBytes(t *testing.T) {
	testbytes := []byte{mfalse, mtrue}

	v, n, err := readBoolBytes(testbytes)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("N should be 1; got %d", n)
	}

	if v != false {
		t.Errorf("Wrong value %t", v)
	}

	//do it again
	v, n, err = readBoolBytes(testbytes[1:])
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("N should be 1; got %d", n)
	}

	if v != true {
		t.Errorf("Wrong value %t", v)
	}
}

func TestReadString(t *testing.T) {
	short := "a short string"
	long := "Salvia pour-over crucifix scenester, fanny pack organic typewriter wayfarers raw denim kale chips chillwave +1. Farm-to-table Thundercats DIY, gastropub meggings viral salvia. Pitchfork skateboard fap Thundercats, forage craft beer Shoreditch direct trade church-key fingerstache High Life tofu roof party Portland distillery. Fanny pack bespoke organic forage, pop-up YOLO wolf leggings Austin selfies crucifix gluten-free sustainable lomo. Polaroid Pinterest Echo Park flexitarian, sartorial VHS mixtape Godard American Apparel retro. Truffaut asymmetrical cliche farm-to-table plaid, 90's leggings Echo Park twee hella shabby chic. Meggings cardigan McSweeney's tofu ethnic."

	testvals := []string{short, long}
	buf := bytes.NewBuffer(nil)

	//write test cases
	for _, x := range testvals {
		writeString(buf, x)
	}

	//test
	for i, x := range testvals {
		t.Logf("Test case %d...", i)
		s, err := readString(buf)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(s, x) {
			t.Errorf("Failed on test case %d", i)
		}
	}
}

func TestReadStringBytes(t *testing.T) {
	short := "a short string"
	long := "Salvia pour-over crucifix scenester, fanny pack organic typewriter wayfarers raw denim kale chips chillwave +1. Farm-to-table Thundercats DIY, gastropub meggings viral salvia. Pitchfork skateboard fap Thundercats, forage craft beer Shoreditch direct trade church-key fingerstache High Life tofu roof party Portland distillery. Fanny pack bespoke organic forage, pop-up YOLO wolf leggings Austin selfies crucifix gluten-free sustainable lomo. Polaroid Pinterest Echo Park flexitarian, sartorial VHS mixtape Godard American Apparel retro. Truffaut asymmetrical cliche farm-to-table plaid, 90's leggings Echo Park twee hella shabby chic. Meggings cardigan McSweeney's tofu ethnic."

	testvals := []string{short, long}

	for i, v := range testvals {
		buf := bytes.NewBuffer(nil)
		writeString(buf, v)

		s, _, err := readStringZeroCopy(buf.Bytes())
		if err != nil {
			t.Errorf("Test case %d: Error: %s", i, err.Error())
		}

		if !reflect.DeepEqual(s, v) {
			t.Errorf("Test case %d: Got %q, expected %q", i, s, v)
		}
	}
}

func TestReadUint(t *testing.T) {
	var short uint64 = 5
	var medium uint64 = 250
	var long uint64 = 300
	var longer uint64 = 100000
	var longest uint64 = 18446744073709501616

	testvals := []uint64{short, medium, long, longer, longest}
	buf := bytes.NewBuffer(nil)

	//write
	for _, x := range testvals {
		writeUint(buf, x)
	}

	//read
	for i, x := range testvals {
		t.Logf("Test case %d...", i)
		s, err := readUint(buf)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(s, x) {
			t.Errorf("%d and %d are not equal", s, x)
		}
	}

}

func TestReadUintBytes(t *testing.T) {
	var short uint64 = 5
	var medium uint64 = 250
	var long uint64 = 300
	var longer uint64 = 100000
	var longest uint64 = 18446744073709501616

	testvals := []uint64{short, medium, long, longer, longest}

	for i, x := range testvals {
		buf := bytes.NewBuffer(nil)
		writeUint(buf, x)

		u, n, err := readUintBytes(buf.Bytes())
		if err != nil {
			t.Errorf("Test case %d: Error: %s", i, err.Error())
		}

		if n != len(buf.Bytes()) {
			t.Errorf("bytes read != full buffer")
		}

		if !reflect.DeepEqual(u, x) {
			t.Errorf("Test case %d: %d != %d", i, u, x)
		}
	}
}

func TestReadInt(t *testing.T) {
	var shortpos int64 = 5               //pos fixint
	var shortneg int64 = -3              //neg fixint
	var mediumpos int64 = 100            //pos int8
	var mediumneg int64 = -85            //neg int8
	var longpos int64 = 15000            //pos int16
	var longneg int64 = -12480           //neg int16
	var longerpos int64 = 1073741824     //pos int32
	var longerneg int64 = -1083321889    //neg int32
	var longestpos int64 = 1099511627776 //pos int64
	var longestneg int64 = -119511627776 //neg int64

	testvals := []int64{shortpos, shortneg, mediumpos, mediumneg, longpos, longneg, longerpos, longerneg, longestpos, longestneg}
	buf := bytes.NewBuffer(nil)
	buf.Grow(90)

	for _, x := range testvals {
		writeInt(buf, x)
	}

	//read
	for i, x := range testvals {
		t.Logf("Test case %d...", i)
		s, err := readInt(buf)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(s, x) {
			t.Errorf("%d and %d are not equal", s, x)
		}
	}
}

func TestReadIntBytes(t *testing.T) {
	var shortpos int64 = 5               //pos fixint
	var shortneg int64 = -3              //neg fixint
	var mediumpos int64 = 100            //pos int8
	var mediumneg int64 = -85            //neg int8
	var longpos int64 = 15000            //pos int16
	var longneg int64 = -12480           //neg int16
	var longerpos int64 = 1073741824     //pos int32
	var longerneg int64 = -1083321889    //neg int32
	var longestpos int64 = 1099511627776 //pos int64
	var longestneg int64 = -119511627776 //neg int64
	testvals := []int64{shortpos, shortneg, mediumpos, mediumneg, longpos, longneg, longerpos, longerneg, longestpos, longestneg}

	for i, x := range testvals {
		buf := bytes.NewBuffer(nil)
		writeInt(buf, x)

		ni, n, err := readIntBytes(buf.Bytes())

		if err != nil {
			t.Errorf("Test case %d: Error: %s", i, err.Error())
		}

		if n != len(buf.Bytes()) {
			t.Errorf("Test case %d: Read %d bytes; should have read %d bytes.", i, n, len(buf.Bytes()))
		}

		if ni != x {
			t.Errorf("Test case %d: %d != %d", i, ni, x)
		}
	}

}

func TestReadFloat(t *testing.T) {
	var smallpos float64 = 3.1                                //float32
	var smallneg float64 = -100 * math.SmallestNonzeroFloat32 //float32
	var largepos float64 = 4 * math.MaxFloat32                //float64
	var largeneg float64 = -0.1 * math.SmallestNonzeroFloat32 //float64

	testvals := []float64{smallpos, smallneg, largepos, largeneg}
	issmall := []bool{true, true, false, false}
	buf := bytes.NewBuffer(nil)
	buf.Grow(28) //theoretical size

	for _, x := range testvals {
		writeFloat(buf, x)
	}

	for i, x := range testvals {
		s, err := readFloat(buf)
		if err != nil {
			t.Fatal(err)
		}
		if issmall[i] {
			x := float32(x)
			if x != float32(s) {
				t.Errorf("Got %v; expected %v", float32(s), x)
			}
			continue
		}
		if x != s {
			t.Errorf("Got %v; expected %v", s, x)
		}
	}
}

func TestReadFloatBytes(t *testing.T) {
	var smallpos float64 = 3.1                                //float32
	var smallneg float64 = -100 * math.SmallestNonzeroFloat32 //float32
	var largepos float64 = 4 * math.MaxFloat32                //float64
	var largeneg float64 = -0.1 * math.SmallestNonzeroFloat32 //float64

	testvals := []float64{smallpos, smallneg, largepos, largeneg}
	issmall := []bool{true, true, false, false}

	for i, x := range testvals {
		buf := bytes.NewBuffer(nil)

		writeFloat(buf, x)

		f, n, err := readFloatBytes(buf.Bytes())

		if err != nil {
			t.Errorf("Test case %d: Error: %s", i, err.Error())
		}

		if n != len(buf.Bytes()) {
			t.Errorf("Test case %d: Read %d bytes; should have read %d.", i, n, len(buf.Bytes()))
		}

		if issmall[i] {
			fg := float32(x)
			if fg != float32(f) {
				t.Errorf("Test case %d: %v != %v", i, fg, float32(f))
			}
		} else {
			if x != f {
				t.Errorf("Test case %d: %v != %v", i, x, f)
			}
		}
	}
}
