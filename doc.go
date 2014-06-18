package flux

import (
	_ "github.com/philhofer/go-flux/fluxd"
	_ "github.com/philhofer/go-flux/log"
	_ "github.com/philhofer/go-flux/msg"
)

/*
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
*/
