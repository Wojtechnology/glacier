package meddb

import "math/big"

type BlockchainDB interface {
	// First time setup to create required tables and indices
	SetupTables() error
	// Writes transaction to backlog table
	WriteTransaction(*Transaction) error
	// Returns transactions currently assigned to given node from backlog table
	GetAssignedTransactions([]byte) ([]*Transaction, error)
	// Deletes given transactions from backlog table
	DeleteTransactions([]*Transaction) error
	// Writes block to block table
	WriteBlock(*Block) error
	// Writes vote to vote table
	WriteVote(*Vote) error
}

type CellAddress struct {
	TableName []byte
	RowId     []byte
	ColId     []byte
	VerId     *big.Int
}

type Transaction struct {
	Hash        []byte
	AssignedTo  []byte // Public key of node this transaction is assigned to
	AssignedAt  *big.Int
	CellAddress *CellAddress
	Data        []byte
}

type Block struct {
	Hash         []byte
	Transactions []*Transaction
	CreatedAt    *big.Int
	Creator      []byte
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

	var cellAddress *CellAddress = nil
	if tx.CellAddress != nil {
		var verId *big.Int = nil
		if tx.CellAddress.VerId != nil {
			verId = tx.CellAddress.VerId
		}

		cellAddress = &CellAddress{
			TableName: tx.CellAddress.TableName,
			RowId:     tx.CellAddress.RowId,
			ColId:     tx.CellAddress.ColId,
			VerId:     verId,
		}
	}

	return &Transaction{
		Hash:        tx.Hash,
		AssignedTo:  tx.AssignedTo,
		AssignedAt:  lastAssigned,
		CellAddress: cellAddress,
		Data:        tx.Data,
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
