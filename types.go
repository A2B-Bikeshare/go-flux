package fluxlog

import (
	"errors"
	capn "github.com/glycerine/go-capnproto"
	"time"
)

var (
	//ErrTypeNotSupported is returned by EntrytoSegment when it encounters an interface{} than cannot be correctly typecast
	ErrTypeNotSupported = errors.New("Type not supported in 'Entry' type.")
)

//EntrytoSegment writes an entry to the segment 's' with columns 'columns' and values 'values'
func EntrytoSegment(s *capn.Segment, name string, entry map[string]interface{}) error {
	ce := NewRootCapEntry(s)
	return setTo(s, ce, name, entry)
}

//LogMsgtoSegment writes a log message in an 'Entry' to a segment.
//This is used for most default 'log' functions, so it is optimized
//to avoid the usage of type reflection. The standard 'EntrytoSegment'
//function does not have the same optimizations
func LogMsgtoSegment(s *capn.Segment, name string, level LogLevel, message string) {
	ce := NewRootCapEntry(s)
	tlist := s.NewTextList(3)
	plist := NewPointsTList(s, 3)
	//set timestamp
	tlist.Set(0, "time")
	tstamppt := NewPointsT(s)
	tstamppt.SetInt(time.Now().Unix())
	capn.PointerList(plist).Set(0, capn.Object(tstamppt))
	//set level
	tlist.Set(1, "level")
	tlevel := NewPointsT(s)
	tlevel.SetInt(int64(level))
	capn.PointerList(plist).Set(1, capn.Object(tlevel))
	//set message
	tlist.Set(2, "message")
	tmsg := NewPointsT(s)
	tmsg.SetText(message)
	capn.PointerList(plist).Set(2, capn.Object(tmsg))

	//set name, columns, points
	ce.SetName(name)
	ce.SetColumns(tlist)
	ce.SetPoints(plist)
}

func setTo(s *capn.Segment, ce CapEntry, name string, vals map[string]interface{}) error {
	ce.SetName(name)
	var err error

	//set column names from 'columns'
	tlist := s.NewTextList(len(vals))
	plist := NewPointsTList(s, len(vals))
	i := 0
	for key, val := range vals {
		tlist.Set(i, key)
		obj := NewPointsT(s)
		switch val.(type) {
		case float64:
			obj.SetFloat(val.(float64))
		case int64:
			obj.SetInt(val.(int64))
		case bool:
			obj.SetBool(val.(bool))
		case string:
			obj.SetText(val.(string))
		default:
			err = ErrTypeNotSupported
			return err
		}
		err = capn.PointerList(plist).Set(i, capn.Object(obj))
		if err != nil {
			return err
		}
		i++
	}
	ce.SetColumns(tlist)
	ce.SetPoints(plist)

	return err
}
