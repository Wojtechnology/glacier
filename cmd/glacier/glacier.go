package main

import (
	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/crypto"
	"github.com/wojtechnology/glacier/loop"
	"github.com/wojtechnology/glacier/meddb"
)

func initBlockchain() (*core.Blockchain, error) {
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

	privKey, err := crypto.NewPrivateKey()
	if err != nil {
		return nil, err
	}

	bc := core.NewBlockchain(
		db,
		bt,
		&core.Node{PubKey: []byte{69}, PrivKey: privKey},
		[]*core.Node{&core.Node{PubKey: []byte{69}}},
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
