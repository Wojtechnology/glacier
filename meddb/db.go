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
}

type Node struct {
	PubKey []byte
}

type CellAddress struct {
	TableName []byte
	RowId     []byte
	ColId     []byte
	VerId     *big.Int
}

type Transaction struct {
	Hash         []byte
	AssignedTo   []byte // Public key of node this transaction is assigned to
	LastAssigned *big.Int
	CellAddress  *CellAddress
	Data         []byte
}

type Block struct {
	Hash         []byte
	Transactions [][]byte
	CreatedAt    *big.Int
	Creator      []byte
}

func (tx *Transaction) Clone() *Transaction {
	var lastAssigned *big.Int = nil
	if tx.LastAssigned != nil {
		lastAssigned = big.NewInt(tx.LastAssigned.Int64())
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
		Hash:         tx.Hash,
		AssignedTo:   tx.AssignedTo,
		LastAssigned: lastAssigned,
		CellAddress:  cellAddress,
		Data:         tx.Data,
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
