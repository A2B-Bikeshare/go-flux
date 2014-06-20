package msg

import (
	"reflect"
	"unsafe"
)

// Read directly from byte slices
// base for zero-copy public methods

func rint8(p []byte) int64 {
	return int64(int8(p[0]))
}

func rint16(p []byte) int64 {
	return int64(int16(p[1]) | (int16(p[0]) << 8))
}

func rint32(p []byte) int64 {
	return int64(int32(p[3]) | (int32(p[2]) << 8) | (int32(p[1]) << 16) | (int32(p[0]) << 24))
}

func rint64(p []byte) int64 {
	return int64(int64(p[7]) |
		(int64(p[6]) << 8) |
		(int64(p[5]) << 16) |
		(int64(p[4]) << 24) |
		(int64(p[3]) << 32) |
		(int64(p[2]) << 40) |
		(int64(p[1]) << 48) |
		(int64(p[0]) << 56))
}

func ruint8(p []byte) uint64 {
	return uint64(uint8(p[0]))
}

func ruint16(p []byte) uint64 {
	return uint64(uint16(p[1]) | (uint16(p[0]) << 8))
}

func ruint32(p []byte) uint64 {
	return uint64(uint32(p[3]) | (uint32(p[2]) << 8) | (uint32(p[1]) << 16) | (uint32(p[0]) << 24))
}

func ruint64(p []byte) uint64 {
	return uint64(uint64(p[7]) |
		(uint64(p[6]) << 8) |
		(uint64(p[5]) << 16) |
		(uint64(p[4]) << 24) |
		(uint64(p[3]) << 32) |
		(uint64(p[2]) << 40) |
		(uint64(p[1]) << 48) |
		(uint64(p[0]) << 56))
}

//value, bytes read, error
func readUintBytes(p []byte) (i uint64, n int, err error) {
	var c byte
	n = 0
	np := len(p)
	if np == 0 {
		err = ErrShortBytes
		return
	}

	c = p[0]
	n++
	//positive fixint
	if (c & 0x80) == 0 {
		i = uint64(uint8(c & 0x7f))
		return
	}

	switch c {
	case muint8:
		if np < 2 {
			err = ErrShortBytes
			return
		}
		i = ruint8(p[1:])
		n++
		return

	case muint16:
		if np < 3 {
			err = ErrShortBytes
			return
		}
		i = ruint16(p[1:])
		n += 2
		return

	case muint32:
		if np < 5 {
			err = ErrShortBytes
			return
		}
		i = ruint32(p[1:])
		n += 4
		return

	case muint64:
		if np < 9 {
			err = ErrShortBytes
			return
		}
		i = ruint32(p[1:])
		n += 8
		return

	default:
		err = ErrBadTag
		return
	}
}

func readBoolBytes(p []byte) (b bool, n int, err error) {
	if len(p) == 0 {
		err = ErrShortBytes
		return
	}
	c := p[0]
	n = 1
	switch c {
	case mtrue:
		b = true
		return
	case mfalse:
		b = false
		return
	default:
		err = ErrBadTag
		return
	}
}

//value, bytes read, error
func readIntBytes(p []byte) (i int64, n int, err error) {
	var c byte
	n = 0
	np := len(p)
	if n == 0 {
		err = ErrShortBytes
		return
	}

	c = p[0]
	n++
	//positive fixint
	if (c & 0x80) == 0 {
		i = int64(int8(c & 0x7f))
		return
	}
	//negative fixint
	if (c & 0xe0) == 0xe0 {
		i = int64(int8(c))
		return
	}

	switch c {
	case mint8:
		if np < 2 {
			err = ErrShortBytes
			return
		}
		i = rint8(p[1:])
		n++
		return
	case mint16:
		if np < 3 {
			err = ErrShortBytes
			return
		}
		i = rint16(p[1:])
		n += 2
		return
	case mint32:
		if np < 5 {
			err = ErrShortBytes
			return
		}
		i = rint32(p[1:])
		n += 4
		return
	case mint64:
		if np < 9 {
			err = ErrShortBytes
			return
		}
		i = rint16(p[1:])
		n += 8
		return
	default:
		err = ErrBadTag
		return
	}
}

func readFloatBytes(p []byte) (f float64, n int, err error) {
	n = 0
	var c byte
	np := len(p)
	if np == 0 {
		err = ErrShortBytes
		return
	}
	c = p[0]
	n++

	switch c {
	case mfloat32:
		if np < 5 {
			err = ErrShortBytes
			return
		}
		fg := uint32(uint32(p[4]) | (uint32(p[3]) << 8) | (uint32(p[2]) << 16) | (uint32(p[1]) << 24))
		g := *(*float32)(unsafe.Pointer(&fg))
		n += 4
		f = float64(g)
		return

	case mfloat64:
		if np < 9 {
			err = ErrShortBytes
			return
		}
		fg := uint64(uint64(p[7]) |
			(uint64(p[6]) << 8) |
			(uint64(p[5]) << 16) |
			(uint64(p[4]) << 24) |
			(uint64(p[3]) << 32) |
			(uint64(p[2]) << 40) |
			(uint64(p[1]) << 48) |
			(uint64(p[0]) << 56))
		f = *(*float64)(unsafe.Pointer(&fg))
		n += 8
		return

	default:
		err = ErrBadTag
		return
	}
}

//note: changes to 'p' will change 's'; this is "unsafe" behavior and
//SHOULD BE USED WITH EXTREME CARE
func readStringZeroCopy(p []byte) (s string, n int, err error) {
	n = 0
	var c byte
	var strlen int
	np := len(p)

	if np == 0 {
		err = ErrShortBytes
		return
	}

	c = p[0]
	n++

	//fixstr shortcut
	if c&0xe0 == 0xa0 {
		strlen = int(c & 0x1f)
		if strlen > 31 {
			panic("Impossible.")
		}
		if np < 1+strlen {
			err = ErrShortBytes
			return
		}

		sh := &reflect.StringHeader{Data: uintptr(unsafe.Pointer(&p[1])), Len: strlen}
		s = *(*string)(unsafe.Pointer(sh))
		n += strlen
		return
	}

	if np < 2 {
		err = ErrShortBytes
		return
	}

	//find strlen
	switch c {
	case mstr8:
		strlen = int(p[1])
		n++

	case mstr16:
		if np < 3 {
			err = ErrShortBytes
			return
		}
		strlen = int(uint32(uint16(p[2]) | (uint16(p[1]) << 8)))
		n += 2

	case mstr32:
		if np < 5 {
			err = ErrShortBytes
			return
		}
		strlen = int(uint32(uint32(p[4]) | (uint32(p[3]) << 8) | (uint32(p[2]) << 16) | (uint32(p[1]) << 24)))
		n += 4

	default:
		err = ErrBadTag
		return
	}
	if np < n+strlen {
		err = ErrShortBytes
		return
	}
	//read from p[n] into *StringHeader; unsafe cast to string
	sh := &reflect.StringHeader{Data: uintptr(unsafe.Pointer(&p[n])), Len: strlen}
	s = *(*string)(unsafe.Pointer(sh))
	n += strlen
	return
}

func readBinZeroCopy(p []byte) (dat []byte, n int, err error) {
	n = 0
	var c byte
	var binlen int
	np := len(p)
	if np < 2 {
		err = ErrShortBytes
		return
	}

	c = p[0]
	n++

	switch c {
	case mbin8:
		binlen = int(int8(p[1]))
		n++
	case mbin16:
		if np < 3 {
			err = ErrShortBytes
			return
		}
		binlen = int(uint16(p[2]) | (uint16(p[1]) << 8))
		n += 2
	case mbin32:
		if np < 5 {
			err = ErrShortBytes
			return
		}
		binlen = int(uint32(p[4]) | (uint32(p[3]) << 8) | (uint32(p[2]) << 16) | (uint32(p[1]) << 24))
		n += 4
	default:
		err = ErrShortBytes
		return
	}
	if np < n+binlen {
		err = ErrShortBytes
		return
	}
	dat = p[n : n+binlen]
	return
}

func readExtZeroCopy(p []byte) (dat []byte, etype int8, n int, err error) {
	np := len(p)
	var c byte
	n = 0
	if np < 2 {
		err = ErrShortBytes
		return
	}

	c = p[0]
	n++

	//fixed cases
	switch c {
	case mfixext1:
		if np < 3 {
			err = ErrShortBytes
			return
		}
		etype = int8(p[1])
		dat = p[2:3]
		n += 2
		return
	case mfixext2:
		if np < 4 {
			err = ErrShortBytes
			return
		}
		etype = int8(p[1])
		dat = p[2:4]
		n += 3
		return
	case mfixext4:
		if np < 6 {
			err = ErrShortBytes
			return
		}
		etype = int8(p[1])
		dat = p[2:6]
		n += 5
		return
	case mfixext8:
		if np < 10 {
			err = ErrShortBytes
			return
		}
		etype = int8(p[1])
		dat = p[2:10]
		n += 9
		return

	case mfixext16:
		if np < 18 {
			err = ErrShortBytes
			return
		}
		etype = int8(p[1])
		dat = p[2:18]
		n += 17
		return
	}

	var datlen int
	//variable cases
	switch c {
	case mext8:
		datlen = int(uint32(p[1]))
		n++
	case mext16:
		datlen = int(uint32(p[2]) | (uint32(p[1]) << 8))
		n += 2
	case mext32:
		datlen = int(uint32(p[4]) | (uint32(p[3]) << 8) | (uint32(p[2]) << 16) | (uint32(p[1]) << 24))
		n += 4
	default:
		err = ErrShortBytes
		return
	}
	etype = int8(p[n])
	n++
	dat = p[n : n+datlen]
	return
}
