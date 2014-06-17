package fluxmap

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	//ErrBadTag blah blah blah
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

func readUint(r Reader) (i uint64, err error) {

	return
}

func readString(r Reader) (s string, err error) {

	return
}

func readBin(r Reader) (p []byte, err error) {

	return
}

func readExt(r Reader) (etype int8, dat []byte, err error) {

	return
}

//returns the length of the map
func readMapHeader(r Reader) (n int) {

	return
}

func readNil(r Reader) (err error) {
	c, err := r.ReadByte()
	if err != nil {
		return
	}
	if c != mnil {
		err = ErrBadTag
	}
	return
}
