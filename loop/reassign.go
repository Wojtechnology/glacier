package loop

import (
	"time"

	"github.com/wojtechnology/glacier/core"
)

const (
	reassignLoopWaitMS = 30000
	reassignStaleAgeNS = 30000 * 1000
)

func ReassignTransactionsLoop(bc *core.Blockchain, errChannel chan<- error) {
	for true {
		err := reassignTransactions(bc)
		if err != nil {
			errChannel <- err
		}
		// TODO: Adjust for time spent
		timeChannel := time.After(time.Millisecond * reassignLoopWaitMS)
		<-timeChannel
	}
}

func reassignTransactions(bc *core.Blockchain) error {
	staleTxs, err := bc.GetStaleTransactions(reassignStaleAgeNS)
	if err != nil {
		return err
	}

	// TODO: Maybe have a bulk write here
	for _, tx := range staleTxs {
		// Essentially an update
		err := bc.AddTransaction(tx)
		if err != nil {
			return err
		}
	}

	return nil
}
