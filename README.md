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
