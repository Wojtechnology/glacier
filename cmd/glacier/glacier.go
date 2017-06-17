package main

import (
	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/loop"
	"github.com/wojtechnology/glacier/meddb"
)

func initBlockchain() (*core.Blockchain, error) {
	db, err := meddb.NewRethinkBlockchainDB([]string{"localhost"}, "prod")
	if err != nil {
		return nil, err
	}
	bc := core.NewBlockchain(
		db,
		&core.Node{PubKey: []byte{69}},
		[]*core.Node{&core.Node{[]byte{69}}},
	)
	return bc, nil
}

func main() {
	bc, err := initBlockchain()
	if err != nil {
		panic(err)
	}

	errChannel := make(chan error)
	go loop.IOLoop(bc, errChannel)
	go loop.ReassignTransactionsLoop(bc, errChannel)
	go loop.AddBlockLoop(bc, errChannel)
	go loop.VoteOnBlocksLoop(bc, errChannel)

	err = <-errChannel
	panic(err)
}
