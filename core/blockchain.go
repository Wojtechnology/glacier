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

	if err := bc.db.WriteTransaction(tx.ToDBTransaction(assignedTo, lastAssigned)); err != nil {
		return err
	}

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
