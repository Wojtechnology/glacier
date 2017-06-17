package loop

import (
	"time"

	"github.com/wojtechnology/glacier/common"
	"github.com/wojtechnology/glacier/core"
)

// TODO: This should be in config
const (
	blockLoopWaitMS      = 1000
	blockMinTransactions = 100
	blockLongestWaitMS   = 5000
)

// TODO: Make better abstraction for loops (sort of like pipeline in BigChainDB)
// Do this when we use rethink changefeeds to trigger a loop
type blockLoopState struct {
	lastBlockNS int64
}

func AddBlockLoop(bc *core.Blockchain, errChannel chan<- error) {
	s := &blockLoopState{lastBlockNS: common.Now()}
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

	nowNS := common.Now()
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
