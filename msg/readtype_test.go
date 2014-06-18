package msg

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

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
