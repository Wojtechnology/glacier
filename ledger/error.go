package ledger

import (
	"fmt"
	"math/big"
)

// Sum of transaction inputs is less than sum of transaction outputs
type InsufficientFundsError struct {
	InputSum  *big.Int
	OutputSum *big.Int
}

func (e *InsufficientFundsError) Error() string {
	return fmt.Sprintf("Insufficient funcs. Input sum: %v, output sum: %v\n",
		e.InputSum, e.OutputSum)
}

// Block is missing its body
type MissingBodyError struct {
	B *Block
}

func (e *MissingBodyError) Error() string {
	return fmt.Sprintf("Block \"%v\" is missing its body\n", e.B)
}
