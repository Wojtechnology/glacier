package ledger

import "github.com/wojtechnology/medblocks/meddb"

type Chain struct {
	DB            meddb.Database
	Genesis       *Block
	HeadCandidate *Block
}

func NewChain(db meddb.Database) (*Chain, error) {
	chain := &Chain{DB: db}
	head, err := GetHeadBlock(db)
	if err != nil {
		return nil, err
	}
	chain.HeadCandidate = head
	return chain, nil
}
