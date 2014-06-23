package main

import (
	"bytes"
	"github.com/A2B-Bikeshare/go-flux/msg"
	"io"
	"net/http"
)

var gcl *http.Client

// DB is the interface that fluxd uses to communicate with a database.
type DB interface {
	// Translate turns flux/msg data into valid data for the
	// database to consume. (Typically, JSON.) Translate
	// should return an error if it encounters an error reading
	// from 'r' or writing to 'w'
	Translate(r msg.Reader, w io.Writer) error
	// Req should return a valid *http.Request to be performed
	// by an http client. 'r' should be used
	// as the body of the request.
	Req(r io.Reader) (*http.Request, error)
	// Validate is used to validate a response from a server
	// after data is sent. Validate() should return
	// a non-nil error to mark the response as failed.
	Validate(*http.Response) error
}

// drives database decoding & writing
func dbHandle(db DB, r msg.Reader) error {
	buf := bytes.NewBuffer(nil)
	err := db.Translate(r, buf)
	if err != nil {
		return err
	}

	req, err := db.Req(buf)
	if err != nil {
		return err
	}

	res, err := gcl.Do(req)
	if err != nil {
		return err
	}
	err = db.Validate(res)
	if err != nil {
		return err
	}
	return nil
}
