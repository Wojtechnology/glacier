package ledger

import "math/big"

type Transaction struct {
	AccountNonce uint64
	Recipient    *Address
	Payload      []byte
	V            *big.Int
	R, S         *big.Int
}
