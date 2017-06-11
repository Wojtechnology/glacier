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

func NewBlockchain(db meddb.BlockchainDB, me *Node, federation []*Node) *Blockchain {
	return &Blockchain{
		db:         db,
		me:         me,
		federation: federation,
	}
}

// --------------
// Blockchain API
// --------------

// Adds transaction to blockchain backlog.
func (bc *Blockchain) AddTransaction(tx *Transaction) error {
	now := time.Now().UTC().UnixNano()
	tx.AssignedTo = bc.randomAssignee(now).PubKey
	tx.AssignedAt = big.NewInt(now)

	if err := bc.db.WriteTransaction(tx.toDBTransaction()); err != nil {
		return err
	}

	return nil
}

// Builds block from transactions currently assigned to this node in the backlog.
// Validates transactions, builds block and writes block to block table.
// Also, delete invalid transactions from backlog and places transactions that depend on unconfirmed
// blocks back into the backlog.
func (bc *Blockchain) BuildBlock() error {
	dbTxs, err := bc.db.GetAssignedTransactions(bc.me.PubKey)
	if err != nil {
		return err
	}

	txs := make([]*Transaction, len(dbTxs))
	for i, dbTx := range dbTxs {
		txs[i] = fromDBTransaction(dbTx)
	}

	validTxs := make([]*Transaction, 0)
	invalidTxs := make([]*Transaction, 0)
	for _, tx := range txs {
		// TODO: Also, there is the case where the transaction depends on an unconfirmed block
		// You will need to place them back into the backlog
		if tx.Valid() {
			validTxs = append(validTxs, tx)
		} else {
			invalidTxs = append(invalidTxs, tx)
		}
	}

	// Remove invalid transactions from backlog
	toDelete := make([]*meddb.Transaction, len(invalidTxs))
	for i, tx := range invalidTxs {
		toDelete[i] = tx.toDBTransaction()
	}
	if err := bc.db.DeleteTransactions(toDelete); err != nil {
		return err
	}

	// TODO: Have some rule for this in the config
	// i.e. if len(validTxs) > MIN_BLOCK_SIZE ||
	//     (timeSinceLastBlock > MAX_TIME && len(validTxs) > 0)
	if len(validTxs) == 0 {
		return nil
	}

	// Create block out of valid transactions and write to database
	now := time.Now().UTC().UnixNano()
	b := &Block{
		Transactions: make([][]byte, len(validTxs)),
		CreatedAt:    big.NewInt(now),
		Creator:      bc.me.PubKey,
	}
	for i, tx := range validTxs {
		// TODO: Store whole transactions instead of just hashes
		b.Transactions[i] = tx.Hash().Bytes()
	}
	if err := bc.db.WriteBlock(b.toDBBlock()); err != nil {
		return err
	}

	// Remove valid transactions from backlog, now that they have been added to block.
	// We want this to happen after the block has been created since even if the program crashes
	// here, these transactions will not be added again (double spend).
	toDelete = make([]*meddb.Transaction, len(invalidTxs))
	for i, tx := range validTxs {
		toDelete[i] = tx.toDBTransaction()
	}
	if err := bc.db.DeleteTransactions(toDelete); err != nil {
		return err
	}

	return nil
}

// Votes on the oldest block that this node is assigned to.
func (bc *Blockchain) VoteOnBlock() error {
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
