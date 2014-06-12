Fluxlog
=====================
Write *all the messages*!
Powered by [NSQ](http://nsq.io/) and [Cap'n Proto](https://kentonv.github.io/capnproto/index.html).

Intro
-------------
Note: Fluxlog is currently under *heavy* development. Please don't use it.

Fluxlog has two parts:
  - Go Package - import 'github.com/philhofer/fluxlog'
  > Sends messages over NSQ to be picked up by a fluxlog client
  - Client
  > Sits in front of your database and writes *all the messages*

Fluxlog is designed to robust, fault-tolerant, and FAST. However, there are
restrictions on the form of your data.

Currently, I'm working on clients for Elasticsearch and InfluxDB. MongoDB is next on the list.
