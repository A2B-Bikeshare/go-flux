package msg

import ()

const (
	mfixint    uint8 = 0x00
	mfixintMAX uint8 = 0x7f

	mnfixint    uint8 = 0xe0
	mnfixintMAX uint8 = 0xff

	mfixmap    uint8 = 0x80
	mfixmapMAX uint8 = 0x8f

	mfixarray    uint8 = 0x90
	mfixarrayMAX uint8 = 0x9f

	mfixstr    uint8 = 0xa0
	mfixstrMAX uint8 = 0xbf

	mnil      uint8 = 0xc0
	mfalse    uint8 = 0xc2
	mtrue     uint8 = 0xc3
	mbin8     uint8 = 0xc4
	mbin16    uint8 = 0xc5
	mbin32    uint8 = 0xc6
	mext8     uint8 = 0xc7
	mext16    uint8 = 0xc8
	mext32    uint8 = 0xc9
	mfloat32  uint8 = 0xca
	mfloat64  uint8 = 0xcb
	muint8    uint8 = 0xcc
	muint16   uint8 = 0xcd
	muint32   uint8 = 0xce
	muint64   uint8 = 0xcf
	mint8     uint8 = 0xd0
	mint16    uint8 = 0xd1
	mint32    uint8 = 0xd2
	mint64    uint8 = 0xd3
	mfixext1  uint8 = 0xd4
	mfixext2  uint8 = 0xd5
	mfixext4  uint8 = 0xd6
	mfixext8  uint8 = 0xd7
	mfixext16 uint8 = 0xd8
	mstr8     uint8 = 0xd9
	mstr16    uint8 = 0xda
	mstr32    uint8 = 0xdb
	marray16  uint8 = 0xdc
	marray32  uint8 = 0xdd
	mmap16    uint8 = 0xde
	mmap32    uint8 = 0xdf
)

// Type represents a base type.
// In the case of ints, uints, and floats,
// the returned value is always a 64bit
// value (e.g. int64, uint64, float64).
// Similarly, large types are always written
// as smaller types (int8, int16, etc.) automatically
// when it is possible to do so.
type Type uint8

const (
	//Int represents an int8, int16, int32, or int64 (or a MessagePack negative 'fixint')
	Int Type = iota
	//Uint represents a uint8, uint16, uint32, or uint64 (or a MessagePack positive 'fixint')
	Uint
	//Map represents a MessagePack map type
	String
	//Nil represents the MessagePack 'nil' type
	Bool
	//Bin represents a MessagePack Bin type
	Bin
	//Ext represents a MessagePack Ext type
	Ext
	//Float represents a float32 or float64 MessagePack type
	Float
)
