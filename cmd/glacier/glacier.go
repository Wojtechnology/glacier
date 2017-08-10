package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/crypto"
	"github.com/wojtechnology/glacier/logging"
	"github.com/wojtechnology/glacier/loop"
	"github.com/wojtechnology/glacier/meddb"
)

func initBlockchain(me *core.Node) (*core.Blockchain, error) {
	addresses := []string{"localhost"}
	database := "prod"

	// Init db that contains meddb
	db, err := meddb.NewRethinkBlockchainDB(addresses, database)
	if err != nil {
		return nil, err
	}

	// Init bigtable that contains cells
	bt, err := meddb.NewRethinkBigtable(addresses, database)
	if err != nil {
		return nil, err
	}

	bc := core.NewBlockchain(
		db,
		bt,
		me,
		[]*core.Node{me},
	)
	return bc, nil
}

func nodeFromFile(path string) *core.Node {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Printf("Error when reading private key: %s\n", err.Error())
		os.Exit(1)
	}

	return core.NewNode(crypto.ParsePrivateKey(data))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: glacier <priv_key_file>")
		os.Exit(1)
	}

	path := os.Args[1]
	bc, err := initBlockchain(nodeFromFile(path))
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
