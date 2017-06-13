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
	// TODO: Lock
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

// Returns list of transactions currently assigned to this node in the backlog.
func (bc *Blockchain) GetMyTransactions() ([]*Transaction, error) {
	dbTxs, err := bc.db.GetAssignedTransactions(bc.me.PubKey)
	if err != nil {
		return nil, err
	}

	txs := make([]*Transaction, len(dbTxs))
	for i, dbTx := range dbTxs {
		txs[i] = fromDBTransaction(dbTx)
	}
	return txs, nil
}

// Builds block from given transactions
// Validates transactions, builds block and writes block to block table.
// Also, delete invalid transactions from backlog and places transactions that depend on unconfirmed
// blocks back into the backlog.
func (bc *Blockchain) BuildBlock(txs []*Transaction) (*Block, error) {
	// TODO: Validate transactions

	// TODO: Have some rule for this in the config
	// i.e. if len(validTxs) > MIN_BLOCK_SIZE ||
	//     (timeSinceLastBlock > MAX_TIME && len(validTxs) > 0)
	if len(txs) == 0 {
		// TODO: Raise error here
		return nil, nil
	}

	// TODO: Lock
	voters := make([][]byte, len(bc.federation))
	for i, node := range bc.federation {
		voters[i] = node.PubKey
	}

	// Create block out of transactions
	b := &Block{
		Transactions: txs,
		CreatedAt:    big.NewInt(now()),
		Creator:      bc.me.PubKey,
		Voters:       voters,
	}
	// TODO: Sign block

	return b, nil
}

// Writes block to block table.
// Assumes that block and all of its transactions have been verified.
// Also, assumes that the block has been signed by this node.
func (bc *Blockchain) WriteBlock(b *Block) error {
	if err := bc.db.WriteBlock(b.toDBBlock()); err != nil {
		return err
	}
	return nil
}

// Builds and signs vote for particular block, given previous block.
func (bc *Blockchain) BuildVote(blockId, prevBlockId []byte, value bool) (*Vote, error) {
	v := &Vote{
		Voter:     bc.me.PubKey,
		VotedAt:   big.NewInt(now()),
		PrevBlock: prevBlockId,
		NextBlock: blockId,
		Value:     value,
	}
	// TODO: Sign vote

	return v, nil
}

// Writes vote to vote table.
// Assumes vote is already signed.
func (bc *Blockchain) WriteVote(v *Vote) error {
	if err := bc.db.WriteVote(v.toDBVote()); err != nil {
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

func now() int64 {
	return time.Now().UTC().UnixNano()
}
