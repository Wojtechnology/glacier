package ledger

import (
	"bytes"
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/wojtechnology/glacier/meddb"
)

type Account struct {
	Nonce   AccountNonce
	Addr    Address
	Balance *big.Int
	// TODO: Storage root (if there is a need for storage)
}

// ------------------------
// Account database helpers
// ------------------------

func buildAccountKey(addr Address) []byte {
	return append([]byte(accountPrefix), addr.Bytes()...)
}

// -----------
// Account API
// -----------

// Writes account object to database using account address as key
func (a *Account) Write(db meddb.Database) error {
	key := buildAccountKey(a.Addr)
	return writeRlp(db, key, a)
}

// Gets account object from database using account address as key
func GetAccount(db meddb.Database, addr Address) (*Account, error) {
	key := buildAccountKey(addr)
	data, err := db.Get(key)
	if err != nil {
		return nil, err
	}

	a := new(Account)
	if err = rlp.Decode(bytes.NewReader(data), a); err != nil {
		return nil, err
	}
	return a, nil
}
