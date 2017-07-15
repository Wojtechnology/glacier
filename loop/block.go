package loop

import (
	"errors"
	"sync"
	"time"

	"github.com/wojtechnology/glacier/common"
	"github.com/wojtechnology/glacier/core"
)

// TODO: This should be in config
const (
	blockLoopWaitMS      = 1000 // Wait time between polling for stale transactions
	blockMinTransactions = 100
	blockLongestWaitMS   = 5000
)

// TODO: Make better abstraction for loops sort of like map/reduce
type blockLoopState struct {
	lastBlockMS int64
	txsMap      map[string]*core.Transaction
	txsMapLock  sync.RWMutex
}

func newBlockLoopState() *blockLoopState {
	return &blockLoopState{
		lastBlockMS: common.Now(),
		txsMap:      make(map[string]*core.Transaction),
	}
}

// Assumes all transactions in txs are distinct
func (state *blockLoopState) addTransactions(txs []*core.Transaction) {
	state.txsMapLock.Lock()
	defer state.txsMapLock.Unlock()

	for _, tx := range txs {
		state.txsMap[tx.Hash().String()] = tx
	}
}

// Returns transactions
func (state *blockLoopState) getTransactions() []*core.Transaction {
	state.txsMapLock.Lock()
	defer state.txsMapLock.Unlock()

	txs := make([]*core.Transaction, len(state.txsMap))
	i := 0
	for _, tx := range state.txsMap {
		txs[i] = tx
		i++
	}

	return txs
}

// Removes given transactions from state
func (state *blockLoopState) removeTransactions(txs []*core.Transaction) {
	state.txsMapLock.Lock()
	defer state.txsMapLock.Unlock()

	for _, tx := range txs {
		delete(state.txsMap, tx.Hash().String())
	}
}

func AddBlockLoop(bc *core.Blockchain, errChannel chan<- error) {
	s := newBlockLoopState()

	cursor, err := bc.GetMyTransactionChangefeed()
	if err != nil {
		errChannel <- err
	}

	txChannel := make(chan bool, 1)

	go func(bc *core.Blockchain, s *blockLoopState, txChannel chan bool, errChannel chan<- error) {
		// Get transactions that were assigned previously.
		txs, err := bc.GetMyTransactions()
		if err != nil {
			errChannel <- err
		}
		s.addTransactions(txs)

		// Trigger an attempted addBlock at the beginning
		txChannel <- true

		tickerChannel := getTickerChannel()
		for {
			var err error
			select {
			case <-tickerChannel:
				err = addBlock(bc, s)
			case <-txChannel:
				err = addBlock(bc, s)
			}
			if err != nil {
				errChannel <- err
			}
		}
	}(bc, s, txChannel, errChannel)

	var res core.TransactionChange
	for cursor.Next(&res) {
		if res.NewTransaction != nil {
			// Update or insert (not delete)
			s.addTransactions([]*core.Transaction{res.NewTransaction})
			txChannel <- true
		}
	}

	errChannel <- errors.New("For some reason the transaction changefeed stopped...\n")
}

func getTickerChannel() <-chan time.Time {
	return time.NewTicker(time.Millisecond * blockLoopWaitMS).C
}

func addBlock(bc *core.Blockchain, s *blockLoopState) error {
	txs := s.getTransactions()

	nowMS := common.Now()
	timePassed := time.Unix(0, nowMS-s.lastBlockMS)
	if len(txs) == 0 || (len(txs) < blockMinTransactions &&
		timePassed.Before(time.Unix(0, blockLongestWaitMS))) {

		return nil
	}
	s.lastBlockMS = nowMS

	validTxs := make([]*core.Transaction, 0)
	invalidTxs := make([]*core.Transaction, 0)
	undecidedTxs := make([]*core.Transaction, 0)

	// Validate transactions
	for _, tx := range txs {
		err := bc.ValidateTransaction(tx)
		if err != nil {
			if _, ok := err.(*core.UndecidedOutputsError); ok {
				// Could be decided later so put these back into backlog
				undecidedTxs = append(undecidedTxs, tx)
			} else {
				// Some other error occurred during validation meaning that tx is invalid
				// TODO: Log error
				invalidTxs = append(invalidTxs, tx)
			}
		} else {
			// Validation succeeded, add this transactions to the block
			validTxs = append(validTxs, tx)
		}
	}

	if len(validTxs) > 0 {
		// Only create a block if we actually have valid transactions
		b, err := bc.BuildBlock(validTxs)
		if err != nil {
			return err
		}

		err = bc.WriteBlock(b)
		if err != nil {
			return err
		}
	}

	// Reassign all undecided transactions
	// TODO: Maybe have a bulk write here
	for _, tx := range undecidedTxs {
		// Essentially an update
		err := bc.AddTransaction(tx)
		if err != nil {
			return err
		}
	}

	// Remove transactions from state, they have all been dealt with at this point
	s.removeTransactions(txs)

	// Happens after writing block, since even if this fails, these transactions will be invalidated
	// later on.
	return bc.DeleteTransactions(append(validTxs, invalidTxs...))
}
