package meddb

import "math/big"

type BlockchainDB interface {
	// First time setup to create required tables and indices
	SetupTables() error

	// Writes transaction to backlog table
	WriteTransaction(*Transaction) error
	// Returns transactions currently assigned to given node from backlog table
	GetAssignedTransactions([]byte) ([]*Transaction, error)
	// Returns transactions older than given time (no order) from backlog table
	GetStaleTransactions(int64) ([]*Transaction, error)
	// Deletes given transactions from backlog table
	DeleteTransactions([]*Transaction) error

	// Writes block to block table
	WriteBlock(*Block) error
	// Returns blocks from block table by block ids
	GetBlocks([][]byte) ([]*Block, error)
	// Returns k oldest blocks from block table starting at given timestamp sorted by increasing
	// CreatedAt timestamp.
	GetOldestBlocks(int64, int) ([]*Block, error)

	// Writes vote to vote table
	WriteVote(*Vote) error
	// Returns all votes for given public key from votes table with the given VotedAt
	GetVotes([]byte, int64) ([]*Vote, error)
	// Returns k most recent votes for given public key from votes table sorted by decreasing
	// VotedAt timestamp.
	GetRecentVotes([]byte, int) ([]*Vote, error)
}

type Transaction struct {
	Hash       []byte
	AssignedTo []byte // Public key of node this transaction is assigned to
	AssignedAt *big.Int
	TableName  []byte
	RowId      []byte
	Cols       map[string]*Cell
}

type Block struct {
	Hash         []byte
	Transactions []*Transaction
	CreatedAt    *big.Int
	Creator      []byte
	Voters       [][]byte
}

type Vote struct {
	Hash      []byte
	Voter     []byte
	VotedAt   *big.Int
	PrevBlock []byte
	NextBlock []byte // Block we are voting on
	Value     bool
}

func (tx *Transaction) Clone() *Transaction {
	var lastAssigned *big.Int = nil
	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}

	var cols map[string]*Cell = nil
	if tx.Cols != nil {
		cols = make(map[string]*Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = cell.Clone()
		}
	}

	return &Transaction{
		Hash:       tx.Hash,
		AssignedTo: tx.AssignedTo,
		AssignedAt: lastAssigned,
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
	}
}

func (b *Block) Clone() *Block {
	var createdAt *big.Int = nil
	if b.CreatedAt != nil {
		createdAt = big.NewInt(b.CreatedAt.Int64())
	}

	return &Block{
		Hash:         b.Hash,
		Transactions: b.Transactions,
		CreatedAt:    createdAt,
		Creator:      b.Creator,
		Voters:       b.Voters,
	}
}

func (v *Vote) Clone() *Vote {
	var votedAt *big.Int = nil
	if v.VotedAt != nil {
		votedAt = big.NewInt(v.VotedAt.Int64())
	}

	return &Vote{
		Hash:      v.Hash,
		Voter:     v.Voter,
		VotedAt:   votedAt,
		PrevBlock: v.PrevBlock,
		NextBlock: v.NextBlock,
		Value:     v.Value,
	}
}
