package ledger

import (
	"bytes"
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/wojtechnology/glacier/crypto"
	"github.com/wojtechnology/glacier/meddb"
)

type TxInput struct {
	Source *TxOutput // Maybe store this as a hash instead
}

type TxOutput struct {
	Cubes  *big.Int
	PubKey []byte
}

type Transaction struct {
	AccountNonce uint64
	V            *big.Int
	R, S         *big.Int
	Inputs       []*TxInput
	Outputs      []*TxOutput
}

// ----------------------
// Block database helpers
// ----------------------

func buildUnspentTxOutputsKey(hash Hash) []byte {
	return append([]byte(unspendTxOutputPrefix), hash.Bytes()...)
}

// ------------
// TxOutput API
// TODO(FUTURE): Make outputs have lock scripts that are unlocked by input scripts.
//               Similar to Bitcoin, will allow for more complex constraints on outputs.
// ------------

func (o *TxOutput) Hash() Hash {
	return rlpHash(o)
}

// Writes a transaction output to the unspent pool
func (o *TxOutput) WriteUnspent(db meddb.Database) error {
	key := buildUnspentTxOutputsKey(o.Hash())
	return writeRlp(db, key, o)
}

// Removes a transaction output from the unspent pool
func (o *TxOutput) DeleteUnspent(db meddb.Database) error {
	key := buildUnspentTxOutputsKey(o.Hash())
	return db.Delete(key)
}

// Gets a transaction output from the unspent pool
func GetUnspentTxOutput(db meddb.Database, hash Hash) (*TxOutput, error) {
	key := buildUnspentTxOutputsKey(hash)
	data, err := db.Get(key)
	if err != nil {
		return nil, err
	}

	o := new(TxOutput)
	if err = rlp.Decode(bytes.NewReader(data), o); err != nil {
		return nil, err
	}
	return o, nil
}

// ---------------
// Transaction API
// ---------------

type TransactionBody struct {
	Inputs  []*TxInput
	Outputs []*TxOutput
}

// Returns true if signature of transaction t is valid on public key of output o, with a hash
// generated from the inputs and outputs of t.
func (t *Transaction) validateSignature(o *TxOutput) bool {
	data := &TransactionBody{Inputs: t.Inputs, Outputs: t.Outputs}
	hash := rlpHash(data)
	return crypto.Verify(hash[:], o.PubKey, t.R, t.S)
}
