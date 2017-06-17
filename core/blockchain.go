package core

import (
	"math/big"
	"math/rand"

	"github.com/wojtechnology/glacier/common"
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
	now := common.Now()
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
	return fromDBTransactions(dbTxs), nil
}

// Returns list of transactions that are at least staleAge old from backlog.
func (bc *Blockchain) GetStaleTransactions(staleAge int64) ([]*Transaction, error) {
	dbTxs, err := bc.db.GetStaleTransactions(common.Now() - staleAge)
	if err != nil {
		return nil, err
	}
	return fromDBTransactions(dbTxs), nil
}

// Proxy to db to delete transactions from backlog.
func (bc *Blockchain) DeleteTransactions(txs []*Transaction) error {
	dbTxs := make([]*meddb.Transaction, len(txs))
	for i, tx := range txs {
		dbTxs[i] = tx.toDBTransaction()
	}
	return bc.db.DeleteTransactions(dbTxs)
}

// Builds block from given transactions.
// Validates transactions, builds and returns block.
// Returns error if some of the transactions are invalid, error contains the invalid transactions.
func (bc *Blockchain) BuildBlock(txs []*Transaction) (*Block, error) {
	// TODO: Validate transactions

	// TODO: Have some rule for this in the config
	// i.e. if len(validTxs) > MIN_BLOCK_SIZE ||
	//     (timeSinceLastBlock > MAX_TIME && len(validTxs) > 0)
	if len(txs) == 0 {
		// TODO: Raise error here, should never be called with zero transactions
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
		CreatedAt:    big.NewInt(common.Now()),
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
		VotedAt:   big.NewInt(common.Now()),
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
