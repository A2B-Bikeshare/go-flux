// Flux for Go
// Powered by NSQ and a custom dialect of MSGPACK
//
// Note: Fluxlog is currently under *heavy* development. Please don't use it.
//
// Why Flux?
// Flux abstracts interaction with NSQ to provide durable data transfer
// between your app and your data consumers (databases, metric analytics services, etc).
// Flux continues to work when your database is down for maintenance or a network partition
// appears between your services. Flux/msg also defines a super-fast, lightweight encoding
// protocol that eliminates the need for compression for small-ish messages. However, you can
// use Flux with any serialization format you want, and NSQ can use Snappy or Deflate
// compression if you need it. (Flux/msg is also great for making the best of your Memcached cluster.)
//
// Fluxlog has three parts:
// 	- flux/msg contains the encode and decode API for flux messages
// 	- flux/log contains the API for writing flux messages to an NSQ daemon
// 	- flux/fluxd contains the API for reading flux messages from an NSQ topic and writing them to a database
package flux

import (
	_ "github.com/A2B-Bikeshare/go-flux/fluxd"
	_ "github.com/A2B-Bikeshare/go-flux/log"
	_ "github.com/A2B-Bikeshare/go-flux/msg"
)
