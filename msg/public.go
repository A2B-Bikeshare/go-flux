package msg

// PackExt represents a MessagePack extension, and has msg.Type = msg.Ext.
// A messagepack extension is simply a tuple of an 8-bit type identifier with arbitary binary data.
type PackExt struct {
	// Type is an 8-bit signed integer. The MessagePack standard dictates that 0 through 127
	// are permitted, while negative values are reserved for future use.
	Type int8
	// Data is the data stored in the extension.
	Data []byte
}

/* Write takes an object and writes it to a Writer

Supported type-Type tuples are:

 - float64 - msg.Float
 - bool - msg.Bool
 - int64 - msg.Int
 - uint64 - msg.Uint
 - string - msg.String
 - []byte - msg.Bin
 - *msg.PackExt - msg.Ext (must be non-nil, otherwise panic)

Each type will be compacted on writing if it
does not require all of its bits to represent itself.
Write returns ErrTypeNotSupported if a bad type is given.
Write returns ErrIncorrectType if the type given does not match the interface{} type.
Alternatively, you can use one of the WriteXxxx() methods provided. */
func WriteInterface(w Writer, v interface{}, t Type) error {
	switch t {
	case String:
		s, ok := v.(string)
		if !ok {
			return ErrIncorrectType
		}
		writeString(w, s)
		return nil
	case Int:
		i, ok := v.(int64)
		if !ok {
			return ErrIncorrectType
		}
		writeInt(w, i)
		return nil
	case Uint:
		u, ok := v.(uint64)
		if !ok {
			return ErrIncorrectType
		}
		writeUint(w, u)
		return nil
	case Float:
		f, ok := v.(float64)
		if !ok {
			return ErrIncorrectType
		}
		writeFloat(w, f)
		return nil
	case Bool:
		t, ok := v.(bool)
		if !ok {
			return ErrIncorrectType
		}
		writeBool(w, t)
		return nil
	case Bin:
		b, ok := v.([]byte)
		if !ok {
			return ErrIncorrectType
		}
		writeBin(w, b)
		return nil
	case Ext:
		ext, ok := v.(*PackExt)
		if !ok {
			return ErrIncorrectType
		}
		writeExt(w, ext.Type, ext.Data)
		return nil
	default:
		return ErrTypeNotSupported
	}
}

//WriteFloat writes a float to a msg.Writer
func WriteFloat(w Writer, f float64) { writeFloat(w, f) }

//WriteBool writes a bool to a msg.Writer
func WriteBool(w Writer, b bool) { writeBool(w, b) }

//WriteInt writes an int to a msg.Writer
func WriteInt(w Writer, i int64) { writeInt(w, i) }

//WriteUint writes a uint to a msg.Writer
func WriteUint(w Writer, u uint64) { writeUint(w, u) }

//WriteString writes a string to a msg.Writer
func WriteString(w Writer, s string) { writeString(w, s) }

//WriteBin writes an arbitrary binary to a msg.Writer
func WriteBin(w Writer, b []byte) { writeBin(w, b) }

//WriteExt writes a messagepack 'extension' (tuple of type, data) to a msg.Writer
func WriteExt(w Writer, etype int8, data []byte) { writeExt(w, etype, data) }

// ReadXxxx() methods try to read values
// from a msg.Reader into a value.
// If the reader reads a leading tag that does not
// translate to the ReadXxxx() method called, it
// unreads the leading byte so that another
// ReadXxxx() method can be attempted and returns ErrBadTag.
//
// ReadFloat tries to read into a float64.
func ReadFloat(r Reader) (f float64, err error) {
	f, err = readFloat(r)
	if err != nil {
		if err == ErrBadTag {
			r.UnreadByte()
		}
	}
	return
}

// ReadInt tries to read into an int64
func ReadInt(r Reader) (i int64, err error) {
	i, err = readInt(r)
	if err != nil {
		if err == ErrBadTag {
			r.UnreadByte()
		}
	}
	return
}

// ReadUint tries to read into a uint64.
func ReadUint(r Reader) (u uint64, err error) {
	u, err = readUint(r)
	if err != nil {
		if err == ErrBadTag {
			r.UnreadByte()
		}
	}
	return
}

// ReadString tries to read into a string.
func ReadString(r Reader) (s string, err error) {
	s, err = readString(r)
	if err != nil {
		if err == ErrBadTag {
			r.UnreadByte()
		}
	}
	return
}

// ReadBool tries to read into a bool.
func ReadBool(r Reader) (b bool, err error) {
	b, err = readBool(r)
	if err != nil {
		if err == ErrBadTag {
			r.UnreadByte()
		}
	}
	return
}

// ReadBin tries to read into a byte slice.
// The slice 'b' is used for buffering in order to avoid allocations,
// but it can safely be nil. Usually 'b' should be a slice
// of an array on the stack.
func ReadBin(r Reader, b []byte) (dat []byte, err error) {
	dat, err = readBin(r, b)
	if err != nil {
		if err == ErrBadTag {
			r.UnreadByte()
		}
	}
	return
}

// ReadExt tries to read into an PackExt.
// The slice 'b' is used for buffering in order to avoid allocations,
// but it can safely be nil. In many cases, 'b' should be
// (part of) a [16]byte or [32]byte on the stack (or the
// "typical" size of the binary that you expect to receive.)
func ReadExt(r Reader, b []byte) (p *PackExt, err error) {
	dat, etype, err := readExt(r, b)
	if err != nil {
		if err == ErrBadTag {
			r.UnreadByte()
		}
		return nil, err
	}
	p = &PackExt{Type: etype, Data: dat}
	return p, nil
}

/* ReadInterface returns an interface{} containing the leading object in the reader,
along with its msg.Type.

Provided no error is returned, the following type assertions on the interface{} should be legal:
 - msg.Int -> int64
 - msg.Uint -> uint64
 - msg.Bool -> bool
 - msg.Ext -> *msg.PackExt
 - msg.Bin -> []byte
 - msg.String -> string
 - msg.Float -> float64	*/
func ReadInterface(r Reader) (v interface{}, t Type, err error) {
	var c byte

	c, err = r.ReadByte()
	if err != nil {
		return
	}

	//fixed encoding cases (fixint, nfixint, fixstr)
	switch {
	//fixint
	case (c & 0x80) == 0:
		t = Int
		v = int64(int8(c & 0x7f))
		return

	//negative fixint
	case (c & 0xe0) == 0xe0:
		t = Int
		v = int64(int8(c))
		return

	//fixstr
	case c&0xe0 == 0xa0:
		t = String
		err = r.UnreadByte()
		if err != nil {
			return
		}
		v, err = readString(r)
		return
	}

	//non-fix cases
	switch c {
	case mfalse:
		t = Bool
		v = false
		return
	case mtrue:
		t = Bool
		v = true
		return
	case mint8, mint16, mint32, mint64:
		t = Int
		err = r.UnreadByte()
		if err != nil {
			return
		}
		v, err = readInt(r)
		return
	case muint8, muint16, muint32, muint64:
		t = Uint
		err = r.UnreadByte()
		if err != nil {
			return
		}
		v, err = readUint(r)
		return
	case mfloat32, mfloat64:
		t = Float
		err = r.UnreadByte()
		if err != nil {
			return
		}
		v, err = readFloat(r)
		return
	case mbin8, mbin16, mbin32:
		t = Bin
		err = r.UnreadByte()
		if err != nil {
			return
		}
		v, err = readBin(r, nil)
		return
	case mfixext1, mfixext2, mfixext4, mfixext8, mfixext16, mext8, mext16, mext32:
		t = Ext
		err = r.UnreadByte()
		if err != nil {
			return
		}
		var etype int8
		var dat []byte
		dat, etype, err = readExt(r, nil)
		if err != nil {
			return
		}
		v = &PackExt{Type: etype, Data: dat}
		return
	case mstr8, mstr16, mstr32:
		t = String
		err = r.UnreadByte()
		if err != nil {
			return
		}
		v, err = readString(r)
		return
	default:
		err = ErrTypeNotSupported
		return
	}
}
