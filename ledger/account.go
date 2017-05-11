package ledger

import (
	"bytes"
	"crypto/ecdsa"
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/wojtechnology/glacier/crypto"
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

// Returns an Account with a randomly generate private key/address
func NewAccount() (*Account, *ecdsa.PrivateKey, error) {
	priv, err := crypto.NewPrivateKey()
	if err != nil {
		return nil, nil, err
	}
	return &Account{
		Nonce:   AccountNonce(EncodeNonce(0)),
		Addr:    AddressFromPubKey(crypto.MarshalPublicKey(&priv.PublicKey)),
		Balance: big.NewInt(0),
	}, priv, nil
}
