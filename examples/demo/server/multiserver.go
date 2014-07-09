package main

import (
  "fmt"
  "github.com/A2B-Bikeshare/go-flux/fluxd"
  "github.com/A2B-Bikeshare/go-flux/msg"
  "os"
  "os/signal"
)

// This is our schema
var TeleSchema = msg.Schema{
  {Name: "name", T: msg.String},
  {Name: "dir", T: msg.String},
  {Name: "val", T: msg.Float},
  {Name: "uid", T: msg.Uint},
  {Name: "chrg", T: msg.Int},
}

// This is our binding for InfluxDB the 'Tele' type
var InfluxBinding = &fluxd.BatchBinding{
  Topic:   "demotopic",
  Channel: "demo_recv",
  Endpoint: &fluxd.InfluxDB{
    Schema: TeleSchema,
    Addr:   "localhost:8083",
    DBname: "test",
  },
}

// This is our binding for Elasticsearch for the 'Tele' type
var ElasticsearchBinding = &fluxd.Binding {
  Topic: "demotopic",
  Channel: "demo_es_recv",
  Endpoint: &fluxd.ElasticsearchDB{
    Schema: TeleSchema,
    Addr: "localhost:9200",
    Index: "test",
    Dtype: "tele",
  },
}

func main() {
  // create server
  srv := &fluxd.Server{
    Lookupdaddrs: []string{"127.0.0.1:4161"},
    UseStdout:    true,
  }

  // bind to stuff
  srv.Bind(ElasticsearchBinding)
  srv.BindBatch(InfluxBinding)

  fmt.Println("Initializing server...")
  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, os.Kill, os.Interrupt)
  go func() {
    <-sigs
    srv.Stop()
  }()
  err := srv.Run()
  if err != nil {
    fmt.Printf("ERROR: %s\n", err.Error())
  } else {
    fmt.Println("Exited normally.")
  }

}
