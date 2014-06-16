package fluxmap

import (
	"errors"
)

var (
	//ErrTypeNotSupported returns when creating a schema with an interface{} of unsupported type
	ErrTypeNotSupported = errors.New("Type not supported as Schema type")
	ErrIncorrectType    = errors.New("Incorrect mapping of Type to type")
)

//Schema represents an ordering of named objects
type Schema []Object

//Object represents a named object of known type
type Object struct {
	T    Type
	Name string
}

//MakeSchema makes a Schema out of a map[string]interface{}
//Supported interface{} values are:
// float64, float32
// uint8, uint16, uint32, uint64
// int8, int16, int32, int64
// bool
// string
// []byte (binary)
func MakeSchema(m map[string]interface{}) (s *Schema, err error) {
	o := make([]Object, len(m))
	i := 0
	for key, val := range m {
		o[i].Name = key
		switch val.(type) {
		case float64, float32:
			o[i].T = Float
		case uint8, uint16, uint32, uint64:
			o[i].T = Uint
		case int8, int16, int32, int64:
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
		i++
	}
	*s = Schema(o)
	return
}

func (s *Schema) Encode(a []interface{}, w Writer) (err error) {
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
