Flux for Go
=====================
Write *all the messages*.
Powered by [NSQ](http://nsq.io/) and a custom dialect of [MessagePack](http://msgpack.org).

Intro
-------------
Note: Fluxlog is currently under *heavy* development. Please don't use it.

Fluxlog has three parts:
  - flux/msg contains the encode and decode API for flux messages
  - flux/log contains the API for writing flux messages to an [NSQ](http://nsq.io) daemon
  - flux/fluxd contains the API for reading flux messages from an [NSQ](http://nsq.io) topic and writing them to a supported database.

Currently, I have plans to implement streaming JSON encoders to turn flux messages into [Elasticsearch](http://elasticsearch.org)- and [InfluxDB](http://influxdb.com)-compatible JSON.
In the long run, I'd like to have MongoDB and Neo4j implemented as well.

Performance
-------------
Fluxlog's performance comes from a couple different design decisions:
  - "Messages" are statically-typed orderings of data.
  - Type reflection is done *once* per message 'type', rather than at every call to Decode(). A message 'type' is called a Schema.
  - Since schemas contain the data keys, the keys themselves are not serialized.
  - Values are packed on writing (e.g. int64(5) is encoded as an int8, and then decoded back to an int64)

Here's how long it takes to encode a message containing a string, int64, uint64, float64, and 3 arbitrary bytes: (MacBook, Intel Core i7; GOMAXPROCS=1)
![benchmark](./BenchmarkEncode.png)

Note that the sum of the sizes of the message values is 50 bytes, and the data is 32 bytes after encoding. The data rate above is calculated from the encoded size (32B).

TL;DR you can saturate your Gigabit connection if you want to.

Examples
-----------
### Struct fluxmsg encoding/decoding
```go
// github.com/philhofer/go-flux/msg/examples/struct_example.go

package main

import (
  "bytes"
  "fmt"
  "github.com/philhofer/go-flux/msg"
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
```
