package ledger

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"

	"github.com/wojtechnology/glacier/crypto"
)

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

// Returns the hash to be used for signing the transaction
func (t *Transaction) SigHash() Hash {
	body := &TransactionBody{To: t.To, Amount: t.Amount}
	return rlpHash(body)
}

// Returns the From address for the transaction derived from the V, R, S signature
func (t *Transaction) From() (Address, error) {
	var emptyAddr Address
	if t.R == nil || t.S == nil || t.V == nil {
		// TODO: Maybe new error for this
		return emptyAddr, errors.New("Transaction is missing a signature\n")
	}

	sig := make([]byte, 65)
	r, s, v := PaddedBytes(t.R, 32), PaddedBytes(t.S, 32), PaddedBytes(t.V, 1)
	hash := t.SigHash()

	if len(r) != 32 {
		return emptyAddr, errors.New(fmt.Sprintf("t.R = %v, should be 32 bytes long\n", r))
	}
	if len(s) != 32 {
		return emptyAddr, errors.New(fmt.Sprintf("t.S = %v, should be 32 bytes long\n", s))
	}
	if len(v) != 1 {
		return emptyAddr, errors.New(fmt.Sprintf("t.V = %v, should be 1 byte long\n", v))
	}

	for i := range r {
		sig[i] = r[i]
	}
	for i := range s {
		sig[32+i] = s[i]
	}
	sig[64] = v[0]

	pub, err := crypto.RetrievePublicKey(hash[:], sig)
	if err != nil {
		return emptyAddr, err
	}

	return AddressFromPubKey(pub), nil
}

// Signs the transaction body and writes the corresponding values to V, R, S
func SignTx(t *Transaction, priv *ecdsa.PrivateKey) (*Transaction, error) {
	hash := t.SigHash()
	sig, err := crypto.Sign(hash[:], priv)
	if err != nil {
		return nil, err
	}
	if len(sig) != 65 {
		return nil, errors.New(fmt.Sprintf("Signature \"%v\" must have a length of 65", sig))
	}

	newT := &Transaction{To: t.To, Amount: t.Amount}
	newT.R = new(big.Int).SetBytes(sig[:32])
	newT.S = new(big.Int).SetBytes(sig[32:64])
	newT.V = new(big.Int).SetBytes([]byte{sig[64]})

	return newT, nil
}
