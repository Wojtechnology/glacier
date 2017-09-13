package main

import (
	"fmt"
	"os"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/logging"
	"github.com/wojtechnology/glacier/loop"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: glacier <priv_key_file>")
		os.Exit(1)
	}

	path := os.Args[1]
	me, err := core.NewNodeFromFile(path)
	if err != nil {
		panic(err)
	}

	bc, err := core.InitBlockchain(me)
	if err != nil {
		panic(err)
	}
	logging.InitLoggers(os.Stdout, os.Stderr)
	logging.Info("Glacier is now running!")

	errChannel := make(chan error)
	go loop.IOLoop(bc, errChannel)
	go loop.ReassignTransactionsLoop(bc, errChannel)
	go loop.AddBlockLoop(bc, errChannel)
	go loop.VoteOnBlocksLoop(bc, errChannel)

	err = <-errChannel
	panic(err)
}
