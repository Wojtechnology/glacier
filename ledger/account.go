package ledger

import "math/big"

type Account struct {
	Nonce   AccountNonce
	Addr    Address
	Balance *big.Int
	// TODO: Storage root (if there is a need for storage)
}
