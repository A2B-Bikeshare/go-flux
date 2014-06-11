package fluxlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

//WriteESJSON writes elasticsearch-compatible JSON
func (s CapEntry) WriteESJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	err = b.WriteByte('{')
	if err != nil {
		return err
	}

	//Write "name:"
	_, err = b.WriteString("\"name\":")
	if err != nil {
		return err
	}

	//Write "[name]"
	_, err = b.WriteString(fmt.Sprintf("%q,", s.Name()))
	if err != nil {
		return err
	}

	//Write all columns/points
	pts := s.Points().ToArray()
	cols := s.Columns().ToArray()
	if len(pts) != len(cols) {
		return errors.New("Columns and Points have different lengths!")
	}
	for i, colname := range cols {
		_, err = b.WriteString(fmt.Sprintf("%q:", colname))
		if err != nil {
			return err
		}

		err = pts[i].WriteNoBufferJSON(b)
		if err != nil {
			return err
		}
		if i < len(cols)-1 {
			err = b.WriteByte(',')
			if err != nil {
				return err
			}
		}
	}

	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
