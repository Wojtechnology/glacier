package ledger

import "github.com/wojtechnology/medblocks/meddb"

type Chain struct {
	DB            meddb.Database
	Genesis       *Block
	HeadCandidate *Block
}

func NewChain(database meddb.Database) *Chain {
	chain := &Chain{DB: database}
	return chain
}
