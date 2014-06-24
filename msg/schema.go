package msg

import (
	"errors"
)

var (
	ErrTypeNotSupported = errors.New("Type not supported as Schema type") // ErrTypeNotSupported returns when creating a schema with an interface{} of unsupported type
	ErrIncorrectType    = errors.New("Incorrect mapping of Type to type") // ErrIncorrectType is returned when value.(type) doesn't match msg.Type
	ErrBadArgs          = errors.New("Bad arguments.")                    // ErrBadArgs is returned when arguments are malformed.
	ErrShortSlice       = errors.New("Slice too short.")                  //ErrShortSlice is returned when an argument slice was too short.
)

// Encoder wraps the Encode() method.
// Encode should marshal information from the calling
// object into a writer.
type Encoder interface {
	Encode(w Writer) error
}

// Decoder wraps the Decode() method.
// Decode should overwrite the information
// contained in the calling object by
// unmarshaling from the reader in Decode.
type Decoder interface {
	Decode(r Reader) error
}

//Schema represents an ordering of named objects
type Schema []Object

//Object represents a named object of known type
type Object struct {
	Name string
	T    Type
}

// Serialize packs the schema itself into a msg
func (s *Schema) SerializeTo(w Writer) {
	n := len(*s)
	//write length
	WriteInt(w, int64(n))

	//write objects
	for _, o := range *s {
		WriteUint(w, uint64(o.T))
		WriteString(w, o.Name)
	}
}

// ReadSchema returns a Schema from the output of s.SerializeTo()
func ReadSchema(r Reader) (s *Schema, err error) {
	n, err := ReadInt(r)
	if err != nil {
		return
	}

	var name string
	var t uint64

	os := make([]Object, n)
	for i := 0; i < int(n); i++ {
		t, err = ReadUint(r)
		if err != nil {
			return
		}
		name, err = ReadString(r)
		if err != nil {
			return
		}

		os[i] = Object{T: Type(uint8(t)), Name: name}
	}
	s = (*Schema)(&os)
	return
}

/* MakeSchema makes a Schema out of a []string and []interface{}.
The 'names' and 'types' slices *must* be the same length.
Supported interface{} values are:

 float64, float32
 uint8, uint16, uint32, uint64
 int8, int16, int32, int64
 bool
 string
 []byte (binary)

Note that even though MakeSchema accepts non-64-bit types, the types used in
Encode() *must* be 64-bit (float64, int64, uint64), because the interface{} is type-asserted
to those types internally. */
func MakeSchema(names []string, types []interface{}) (s *Schema, err error) {
	if len(names) != len(types) {
		err = ErrBadArgs
		return
	}
	o := make([]Object, len(names))

	for i, kind := range types {
		o[i].Name = names[i]
		switch kind.(type) {
		case float32, float64:
			o[i].T = Float
		case uint, uint8, uint16, uint32, uint64:
			o[i].T = Uint
		case int, int8, int16, int32, int64:
			o[i].T = Int
		case bool:
			o[i].T = Bool
		case string:
			o[i].T = String
		case []byte:
			o[i].T = Bin
		default:
			return nil, ErrTypeNotSupported
		}
	}
	s = (*Schema)(&o)
	return
}

// DecodeToSlice reads values from a msg.Reader into a []interface{}, provided that
// the provided slice is long enough. (If not, ErrShortSlice is returned.)
// DecodeToSlice is a higher-performance alternative to DecodeToMap.
func (s *Schema) DecodeToSlice(r Reader, v []interface{}) error {
	if len(v) < len(*s) {
		return ErrShortSlice
	}
	var t Type         //type
	var ns interface{} //value
	var err error      //error

	for i, o := range *s {
		t = o.T
		switch t {

		case String:
			ns, err = readString(r)
			if err != nil {
				return err
			}
			v[i] = ns
			continue

		case Int:
			ns, err = readInt(r)
			if err != nil {
				return err
			}
			v[i] = ns
			continue

		case Uint:
			ns, err = readUint(r)
			if err != nil {
				return err
			}
			v[i] = ns
			continue

		case Float:
			ns, err = readFloat(r)
			if err != nil {
				return err
			}
			v[i] = ns
			continue

		case Bin:
			var dat []byte
			var bs [32]byte //try to avoid allocations for small bins
			dat, err = readBin(r, bs[:32])
			if err != nil {
				return err
			}
			v[i] = dat
			continue

		case Ext:
			var dat []byte
			var etype int8
			var bs [32]byte //try to avoid allocations for small exts
			dat, etype, err = readExt(r, bs[:32])
			if err != nil {
				return err
			}
			v[i] = &PackExt{EType: etype, Data: dat}
			continue

		default:
			err = ErrIncorrectType
			return err

		}
	}
	return nil
}

// DecodeToSliceZeroCopy reads the data from 'p' directly into values in 'v'.
// The length of 'v' must be greater than or equal to the length of *s. Also,
// the values in v point to data in 'p', so those values are only "safe" as long
// as 'p' remains untouched. This is both "dangerous" and highly performant. Use
// at your own risk.
func (s *Schema) DecodeToSliceZeroCopy(p []byte, v []interface{}) error {
	var nn int //total bytewise progress

	//check for sanity
	if len(v) < len(*s) {
		return ErrShortSlice
	}

	for i, o := range *s {
		switch o.T {
		case String:
			s, n, err := readStringZeroCopy(p[nn:])
			if err != nil {
				return err
			}
			v[i] = s
			nn += n

		case Int:
			in, n, err := readIntBytes(p[nn:])
			if err != nil {
				return err
			}
			v[i] = in
			nn += n

		case Uint:
			uin, n, err := readUintBytes(p[nn:])
			if err != nil {
				return err
			}
			v[i] = uin
			nn += n

		case Float:
			f, n, err := readFloatBytes(p[nn:])
			if err != nil {
				return err
			}
			v[i] = f
			nn += n

		case Bool:
			b, n, err := readBoolBytes(p[nn:])
			if err != nil {
				return err
			}
			v[i] = b
			nn += n

		case Bin:
			dat, n, err := readBinZeroCopy(p[nn:])
			if err != nil {
				return err
			}
			v[i] = dat
			nn += n

		case Ext:
			dat, etype, n, err := readExtZeroCopy(p[nn:])
			if err != nil {
				return err
			}
			p := &PackExt{EType: etype, Data: dat}
			v[i] = p
			nn += n

		default:
			return ErrIncorrectType
		}

	}
	return nil //schema is nil...
}

// DecodeToMap uses a schema to decode a fluxmsg stream into a map[string]interface{}.
// The map keys are the Name fields of each msg.Object in the msg.Schema.
func (s *Schema) DecodeToMap(r Reader, m map[string]interface{}) error {
	var t Type
	var n string
	var ns interface{}
	var err error
	for _, o := range *s {
		t = o.T
		n = o.Name
		switch t {

		case String:
			ns, err = readString(r)
			if err != nil {
				return err
			}
			m[n] = ns

		case Int:
			ns, err = readInt(r)
			if err != nil {
				return err
			}
			m[n] = ns

		case Uint:
			ns, err = readUint(r)
			if err != nil {
				return err
			}
			m[n] = ns

		case Float:
			ns, err = readFloat(r)
			if err != nil {
				return err
			}
			m[n] = ns

		case Bin:
			var bs [32]byte
			var dat []byte
			dat, err = readBin(r, bs[:32])
			if err != nil {
				return err
			}
			m[n] = dat

		case Ext:
			var bs [32]byte
			var dat []byte
			var etype int8
			dat, etype, err = readExt(r, bs[:32])
			if err != nil {
				return err
			}
			m[n] = &PackExt{EType: etype, Data: dat}

		default:
			err = ErrIncorrectType
			return err

		}
	}
	return nil
}

//Encode uses a schema to encode a slice-of-interface to a writer.
func (s *Schema) EncodeSlice(a []interface{}, w Writer) (err error) {
	for i, v := range a {
		err = encode(v, (*s)[i], w)
		if err != nil {
			return
		}
	}
	return
}

func encode(v interface{}, o Object, w Writer) error {
	switch o.T {
	case Float:
		f, ok := v.(float64)
		if !ok {
			return ErrIncorrectType
		}
		writeFloat(w, f)
		return nil
	case Uint:
		i, ok := v.(uint64)
		if !ok {
			return ErrIncorrectType
		}
		writeUint(w, i)
		return nil
	case Int:
		i, ok := v.(int64)
		if !ok {
			return ErrIncorrectType
		}
		writeInt(w, i)
		return nil
	case Bool:
		b, ok := v.(bool)
		if !ok {
			return ErrIncorrectType
		}
		writeBool(w, b)
		return nil
	case String:
		s, ok := v.(string)
		if !ok {
			return ErrIncorrectType
		}
		writeString(w, s)
		return nil
	case Bin:
		bs, ok := v.([]byte)
		if !ok {
			return ErrIncorrectType
		}
		writeBin(w, bs)
		return nil
	default:
		return ErrTypeNotSupported
	}
}
