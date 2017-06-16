package main

import (
	"net/http"
	"time"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/handler"
	"github.com/wojtechnology/glacier/meddb"
)

const (
	blockLoopWaitMS      = 1000
	blockMinTransactions = 100
	blockLongestWaitMS   = 5000
)

// Should only be called once
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

func ioLoop(bc *core.Blockchain, errChannel chan<- error) {
	handler.SetBlockchain(bc)
	handler.SetupRoutes()
	http.ListenAndServe(":8000", nil)
}

// TODO: Make better abstraction for state with loops
type blockLoopState struct {
	lastBlockNS int64
}

func addBlockLoop(bc *core.Blockchain, errChannel chan<- error) {
	s := &blockLoopState{lastBlockNS: now()}
	for true {
		err := addBlock(bc, s)
		if err != nil {
			errChannel <- err
		}
		// TODO: Adjust for time spent
		timeChannel := time.After(time.Millisecond * blockLoopWaitMS)
		<-timeChannel
	}
}

func addBlock(bc *core.Blockchain, s *blockLoopState) error {
	txs, err := bc.GetMyTransactions()
	if err != nil {
		return err
	}

	nowNS := now()
	timePassed := time.Unix(0, nowNS-s.lastBlockNS)
	if len(txs) == 0 || (len(txs) < blockMinTransactions &&
		timePassed.Before(time.Unix(0, blockLongestWaitMS*int64(time.Millisecond)))) {

		return nil
	}
	s.lastBlockNS = nowNS

	b, err := bc.BuildBlock(txs)
	if err != nil {
		// TODO: Handle case where there are invalid transactions and delete them from backlog
		return err
	}

	err = bc.WriteBlock(b)
	if err != nil {
		return err
	}

	// Happens after writing block, since even if this fails, these transactions will be invalidated
	// later on.
	return bc.DeleteTransactions(txs)
}

func now() int64 {
	return time.Now().UTC().UnixNano()
}

func reassignTransactionsLoop(bc *core.Blockchain, errChannel chan<- error) {
}

func voteOnBlocksLoop(bc *core.Blockchain, errChannel chan<- error) {
}

func main() {
	bc, err := initBlockchain()
	if err != nil {
		panic(err)
	}

	errChannel := make(chan error)
	go ioLoop(bc, errChannel)
	go reassignTransactionsLoop(bc, errChannel)
	go addBlockLoop(bc, errChannel)
	go voteOnBlocksLoop(bc, errChannel)

	err = <-errChannel
	panic(err)
}
