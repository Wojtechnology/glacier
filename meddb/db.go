package meddb

import "math/big"

type BlockchainDB interface {
	SetupTables() error                  // First time setup to create required tables and indices
	WriteTransaction(*Transaction) error // Writes transaction to backlog
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
