Fluxlog
=====================
Write *all the messages*!
Powered by [NSQ](http://nsq.io/) and a custom dialect of [MessagePack](http://msgpack.org).

Intro
-------------
Note: Fluxlog is currently under *heavy* development. Please don't use it.

Fluxlog has two parts:
  - Go Package - import 'github.com/philhofer/fluxlog' - Sends messages over NSQ to be picked up by a fluxlog client
  - Client - Sits in front of your database and writes *all the messages*

Fluxlog is designed to robust, fault-tolerant, and FAST. However, there are
restrictions on the form of your data.

Currently, I'm working on clients for Elasticsearch and InfluxDB. MongoDB is next on the list. Each client will know how to parse
a fluxmap schema directly into JSON without using an intermediate Go representation, which should
make it inherently faster than most existing solutions.

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
