package ledger

import "math/big"

type Transaction struct {
	AccountNonce uint64
	V            *big.Int
	R, S         *big.Int
	To           []byte
	Amount       *big.Int
}

// ---------------
// Transaction API
// ---------------

type TransactionBody struct {
	To     []byte
	Amount *big.Int
}
