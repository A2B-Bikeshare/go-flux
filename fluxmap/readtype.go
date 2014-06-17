package fluxmap

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	//ErrBadTag is returned when a readXxxx method is used on an incorrect flag
	ErrBadTag = errors.New("Bad tag.")
)

//Reader must implement io.Reader and io.ByteReader, and optionally io.StringReader (for speed)
type Reader interface {
	io.Reader
	io.ByteReader
}

func readBool(r Reader) (b bool, err error) {
	c, err := r.ReadByte()
	if err != nil {
		return
	}
	switch c {
	case mtrue:
		b = true
	case mfalse:
		b = false
	default:
		err = ErrBadTag
	}
	return
}

func readInt(r Reader) (i int64, err error) {
	c, err := r.ReadByte()
	if err != nil {
		return
	}
	//positive fixint
	if (c & 0x80) == 0 {
		i = int64(c & 0x7f)
		return
	}
	//negative fixint
	if (c & 0xe0) == 0 {
		i = int64(c & 0x1f)
		return
	}

	switch c {
	case mint8:
		n := int8(0)
		err = binary.Read(r, binary.BigEndian, &n)
		i = int64(n)
		return
	case mint16:
		n := int16(0)
		err = binary.Read(r, binary.BigEndian, &n)
		i = int64(n)
		return
	case mint32:
		n := int32(0)
		err = binary.Read(r, binary.BigEndian, &n)
		i = int64(n)
		return
	case mint64:
		err = binary.Read(r, binary.BigEndian, &i)
		return
	default:
		err = ErrBadTag
		return
	}
}
