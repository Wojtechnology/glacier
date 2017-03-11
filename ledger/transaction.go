package ledger

import "math/big"

type Transaction struct {
	AccountNonce uint64
	V            *big.Int
	R, S         *big.Int
}
