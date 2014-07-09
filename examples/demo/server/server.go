package main

import (
	"fmt"
	"github.com/A2B-Bikeshare/go-flux/fluxd"
	"github.com/A2B-Bikeshare/go-flux/msg"
)

// This is our schema
var TeleSchema = msg.Schema{
	{Name: "name", T: msg.String},
	{Name: "dir", T: msg.String},
	{Name: "val", T: msg.Float},
	{Name: "uid", T: msg.Uint},
	{Name: "chrg", T: msg.Int},
}

// This is our binding for the 'Tele' type
var TeleBinding = &fluxd.BatchBinding{
	Topic:   "demotopic",
	Channel: "demo_recv",
	Endpoint: &fluxd.InfluxDB{
		Schema: TeleSchema,
		Addr:   "localhost:8083",
		DBname: "test",
	},
}

func main() {
	srv := &fluxd.Server{
		Lookupdaddrs: []string{"127.0.0.1:4161"},
		UseStdout:    true,
	}
	srv.BindBatch(TeleBinding)
	fmt.Println("Initializing server...")
	err := srv.Run()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
	} else {
		fmt.Println("Exited normally.")
	}
}
