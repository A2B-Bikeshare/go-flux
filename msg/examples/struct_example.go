package main

import (
	"bytes"
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/msg"
)

// Person - the struct we want to encode/decode
type Person struct {
	Name string
	Age  int64 // needs to be int64 to interact transparently with msg.WriteInt() and msg.ReadInt()
}

// WriteFluxMsg - a method to encode the struct as a flux msg
func (p *Person) WriteFluxMsg(w msg.Writer) {
	msg.WriteString(w, p.Name)
	msg.WriteInt(w, p.Age)
}

// FromFluxMsg - a method to decode the struct as a flux msg
func (p *Person) FromFluxMsg(r msg.Reader) error {

	// Note that the order of reads
	// is the same as the order of writes.
	// Any other arrangement will fail.
	// fluxmsg encoding/decoding is always typed AND ordered.

	newname, err := msg.ReadString(r)
	if err != nil {
		return err
	}
	newage, err := msg.ReadInt(r)
	if err != nil {
		return err
	}
	p.Name, p.Age = newname, newage
	return nil
}

func main() {
	//make a Person; write to a buffer
	bob := &Person{Name: "Bob", Age: 32}
	buf := bytes.NewBuffer(nil)
	//*bytes.Buffer implements the msg.Writer interface
	bob.WriteFluxMsg(buf)

	//Print the hex-encoded representation of the message
	fmt.Printf("Bob encoded to '%x'\n", buf.Bytes())
	// Output:
	// Bob encoded to 'a3426f6220'

	//Make a new Person; read in values from a buffer
	newbob := &Person{}
	err := newbob.FromFluxMsg(buf)
	if err != nil {
		fmt.Println(err)
		return
	}

	//Print the value of the new person
	fmt.Printf("New Bob decoded as %v\n", *newbob)
	// Output:
	// New Bob decoded as {Bob 32}
}
