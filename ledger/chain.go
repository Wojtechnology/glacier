package ledger

import (
	"errors"

	"github.com/wojtechnology/glacier/meddb"
)

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

// Adds transaction to head block in chain, gives reward to owner
// TODO: Add transaction fee and return state change
func (c *Chain) AddTransaction(t *Transaction) (*Transaction, error) {
	from, err := t.From()
	if err != nil {
		return nil, err
	}

	fromAcc, err := GetAccount(c.DB, from)
	if err != nil {
		return nil, err
	}

	if t.Amount.Cmp(fromAcc.Balance) == 1 {
		// TODO: Maybe new error for insufficient funds
		return nil, errors.New("Not enough account balance to complete transaction\n")
	}

	toAcc, err := GetAccount(c.DB, t.To)
	if err != nil {
		return nil, err
	}

	fromAcc.Balance.Sub(fromAcc.Balance, t.Amount)
	toAcc.Balance.Add(toAcc.Balance, t.Amount)

	err = fromAcc.Write(c.DB)
	if err != nil {
		return nil, err
	}

	err = toAcc.Write(c.DB)
	if err != nil {
		return nil, err
	}

	err = c.HeadCandidate.AddTransaction(t)
	if err != nil {
		return nil, err
	}
	err = c.HeadCandidate.WriteHead(c.DB)
	if err != nil {
		return nil, err
	}

	// TODO: Figure out how to test this
	c.DB.Commit()

	return t, nil
}
