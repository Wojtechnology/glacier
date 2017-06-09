package core

import (
	"math/big"

	"github.com/wojtechnology/glacier/meddb"
)

type Block struct {
	Transactions [][]byte // Contains hashes of all contained transactions
	CreatedAt    *big.Int // Time at which block was created, will be used to determine order
	Creator      []byte
}

// ---------
// Block API
// ---------

func (b *Block) Hash() Hash {
	// TODO: Think about this, maybe we want to hash a subset
	return rlpHash(b)
}

func (b *Block) toDBBlock() *meddb.Block {
	var createdAt *big.Int = nil
	if b.CreatedAt != nil {
		createdAt = big.NewInt(b.CreatedAt.Int64())
	}
	// TODO(wojtek): Maybe make copies here
	return &meddb.Block{
		Hash:         b.Hash().Bytes(),
		Transactions: b.Transactions,
		CreatedAt:    createdAt,
		Creator:      b.Creator,
	}
}

func fromDBBlock(b *meddb.Block) *Block {
	var createdAt *big.Int = nil
	if b.CreatedAt != nil {
		createdAt = big.NewInt(b.CreatedAt.Int64())
	}
	// TODO(wojtek): Maybe make copies here
	return &Block{
		Transactions: b.Transactions,
		CreatedAt:    createdAt,
		Creator:      b.Creator,
	}
}
