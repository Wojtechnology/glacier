package ledger

import "github.com/wojtechnology/glacier/meddb"

type Chain struct {
	DB            meddb.Database
	Genesis       *Block
	HeadCandidate *Block
}

// Builds and returns a chain from the existing database
// If the chain does not yet exist, creates a chain containing only genesis
func NewChain(db meddb.Database) (*Chain, error) {
	c := &Chain{DB: db}

	gen, err := GetOrCreateGenesisBlock(db)
	if err != nil {
		return nil, err
	}
	c.Genesis = gen

	head, err := GetHeadBlock(db)
	if err != nil {
		// Head block not found, set genesis as head block
		err = gen.WriteHead(db)
		if err != nil {
			return nil, err
		}
		head = gen
	}
	c.HeadCandidate = head
	return c, nil
}
