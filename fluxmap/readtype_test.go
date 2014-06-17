package fluxmap

import (
	"bytes"
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
