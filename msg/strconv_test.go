package msg

import (
	"bytes"
	"math"
	"math/rand"
	"strconv"
	"testing"
)

func TestItoa(t *testing.T) {
	ints := make([]int64, 10000)
	for i := range ints {
		ints[i] = rand.Int63n(math.MaxInt64) - (math.MaxInt64 / 2)
	}
	// test
	buf := bytes.NewBuffer(nil)
	for _, val := range ints {
		writeItoa(val, buf)
		n, err := strconv.ParseInt(buf.String(), 10, 64)
		if err != nil {
			t.Errorf("Parse error: %s", err.Error())
		} else if n != val {
			t.Errorf("Value error: %d != %d", n, val)
		}
		buf.Reset()
	}
}

func TestUtoa(t *testing.T) {
	uints := make([]uint64, 10000)
	for i := range uints {
		uints[i] = uint64(rand.Int63n(math.MaxInt64))
	}
	buf := bytes.NewBuffer(nil)
	for _, val := range uints {
		writeUtoa(val, buf)
		n, err := strconv.ParseInt(buf.String(), 10, 64)
		if err != nil {
			t.Errorf("Parse error: %s", err.Error())
		} else if uint64(n) != val {
			t.Errorf("Value error: %d != %d", n, val)
		}
		buf.Reset()
	}
}
