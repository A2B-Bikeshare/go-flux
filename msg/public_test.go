package msg

import (
	"bytes"
	"reflect"
	"testing"
)

// this should cover all of the public read/write methods
func TestReadWriteInterfacePublic(t *testing.T) {
	testTypes := []Type{Bool, Float, Int, Uint, String, Bin, Ext}
	testVals := make([]interface{}, 7)

	testVals[0] = true
	testVals[1] = float64(3.141590000000)
	testVals[2] = int64(-52000)
	testVals[3] = uint64(1000000)
	testVals[4] = "A test string. Mmmm."
	testVals[5] = []byte{1, 14, 199, 7}
	testVals[6] = &PackExt{Type: 4, Data: []byte{7, 8, 9}}

	for i, val := range testVals {
		buf := bytes.NewBuffer(nil)
		err := WriteInterface(buf, val, testTypes[i])
		if err != nil {
			t.Errorf("Test case %d: %s", i, err)
			continue
		}

		v, tt, err := ReadInterface(buf)
		if err != nil {
			t.Errorf("Test case %d: %s", i, err)
			continue
		}

		if tt != testTypes[i] {
			t.Errorf("Test case %d: Expected type %d, got type %d", i, testTypes[i], tt)
		}

		if tt == Float {
			continue
		}

		if !reflect.DeepEqual(val, v) {
			t.Errorf("Test case %d: Got %v, expected %v", i, v, val)
		}
	}

}
