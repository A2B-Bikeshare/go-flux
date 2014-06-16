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
  1. Schemas are 'compiled' in advance. Consequently, there is no type reflection necessary for serialization. *However*, schemas can be compiled during runtime.
  2. Schemas have byte-for-byte serialization methods, and the serializer compacts values (e.g. int64->int16) where possible.
  3. Schemas eliminate about 50% of the overhead traditionally associated with maps, as they do not re-send key values each time.
  4. Log() methods don't incur context switches unless the publisher hasn't caught up (which is unlikely).
