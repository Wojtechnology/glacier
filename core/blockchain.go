package core

import (
	"math/big"
	"math/rand"
	"time"

	"github.com/wojtechnology/glacier/meddb"
)

type Blockchain struct {
	db         meddb.BlockchainDB // Stores db data structures
	me         *Node              // This node
	federation []*Node            // All other nodes in the network
}

func NewBlockchain(db meddb.BlockchainDB, me *Node) *Blockchain {
	return &Blockchain{db: db, me: me, federation: []*Node{&Node{PubKey: []byte{42}}}}
}

// --------------
// Blockchain API
// --------------

func (bc *Blockchain) AddTransaction(tx *Transaction) error {
	now := time.Now().UTC().UnixNano()
	assignedTo := bc.randomAssignee(now).PubKey
	lastAssigned := big.NewInt(now)

	if err := bc.db.WriteTransaction(tx.toDBTransaction(assignedTo, lastAssigned)); err != nil {
		return err
	}

	return nil
}

func (bc *Blockchain) BuildBlock() error {
	dbTxs, err := bc.db.GetAssignedTransactions(bc.me.PubKey)
	if err != nil {
		return err
	}

	txs := make([]*Transaction, len(dbTxs))
	for i, dbTx := range dbTxs {
		txs[i] = fromDBTransaction(dbTx)
	}

	_ = make([]*Transaction, 0)
	_ = make([]*Transaction, 0)
	for _, tx := range txs {
		if tx.Valid() {
		} else {
		}
	}

	// Delete invalid transactions
	// Create block out of valid transactions
	return nil
}

// -------
// Helpers
// -------

// Returns a random node to assign a transaction to that is not this node.
func (bc *Blockchain) randomAssignee(seed int64) *Node {
	rand.Seed(seed)
	return bc.federation[rand.Intn(len(bc.federation))]
}
