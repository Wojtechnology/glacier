package main

import (
	"net/http"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/handler"
	"github.com/wojtechnology/glacier/meddb"
)

// Should only be called once
func initBlockchain() (*core.Blockchain, error) {
	db, err := meddb.NewRethinkBlockchainDB([]string{"localhost"}, "prod")
	if err != nil {
		return nil, err
	}
	bc := core.NewBlockchain(db, &core.Node{PubKey: []byte{69}})
	return bc, nil
}

func ioLoop(bc *core.Blockchain, errChannel chan<- error) {
	handler.SetBlockchain(bc)
	handler.SetupRoutes()
	http.ListenAndServe(":8000", nil)
}

func reassignTransactionsLoop(errChannel chan<- error) {
}

func addBlocksLoop(errChannel chan<- error) {
}

func voteOnBlocksLoop(errChannel chan<- error) {
}

func main() {
	bc, err := initBlockchain()
	if err != nil {
		panic(err)
	}

	errChannel := make(chan error)
	go ioLoop(bc, errChannel)
	go reassignTransactionsLoop(errChannel)
	go addBlocksLoop(errChannel)
	go voteOnBlocksLoop(errChannel)

	err = <-errChannel
	panic(err)
}
