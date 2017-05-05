package ledger

import (
	"math/big"

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
// Returns modified transaction with reward added.
func (c *Chain) AddTransaction(t *Transaction, ownerPubKey []byte) (*Transaction, error) {
	inOutputs := make([]*TxOutput, 0)
	inSum := big.NewInt(0)
	// TODO(FUTURE): Batch
	for _, input := range t.Inputs {
		// Check if source is actually unspent
		inOutput, err := GetUnspentTxOutput(c.DB, input.Source.Hash())
		if err != nil {
			return nil, err
		}

		// Validate that the input can be used using signature
		if !t.validateSignature(inOutput) {
			return nil, &InvalidSignatureError{T: t, O: inOutput}
		}

		inSum.Add(inSum, inOutput.Cubes)
		inOutputs = append(inOutputs, inOutput)
	}

	// Calculate reward
	outSum := big.NewInt(0)
	for _, output := range t.Outputs {
		outSum.Add(outSum, output.Cubes)
	}

	leftover := big.NewInt(0)
	leftover.Sub(inSum, outSum)
	if leftover.Cmp(big.NewInt(0)) < 0 {
		return nil, &InsufficientFundsError{InputSum: inSum, OutputSum: outSum}
	}

	// Add reward to transaction
	reward := &TxOutput{Cubes: leftover, PubKey: ownerPubKey}
	t.Outputs = append(t.Outputs, reward)

	// TODO(FUTURE): Batch - maybe a transaction would be best so restoring would be trivial
	// Add transaction to head block
	err := c.HeadCandidate.AddTransaction(t)
	if err != nil {
		return nil, err
	}

	// Remove spent outputs from unspent pool
	for _, inOutput := range inOutputs {
		err := inOutput.DeleteUnspent(c.DB)
		if err != nil {
			c.reverseTransaction(t)
			return nil, err
		}
	}

	// Add new outputs to unspent pool
	for _, output := range t.Outputs {
		err := output.WriteUnspent(c.DB)
		if err != nil {
			c.reverseTransaction(t)
			return nil, err
		}
	}

	return t, nil
}

// Helper that reverses the unspent pool operations in case of an error
// TODO(FUTURE): Batch
func (c *Chain) reverseTransaction(t *Transaction) {
	for _, input := range t.Inputs {
		input.Source.DeleteUnspent(c.DB) // Ignore errors on purpose
	}

	for _, output := range t.Outputs {
		output.DeleteUnspent(c.DB) // Ignore errors on purpose
	}
}
