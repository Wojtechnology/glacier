package ledger

// NOTE: Not used at this point, might still be useful later

import (
	"bytes"
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/wojtechnology/glacier/meddb"
)

type TxInput struct {
	Source *TxOutput // Maybe store this as a hash instead
}

type TxOutput struct {
	Cubes  *big.Int
	PubKey []byte
}

// -------------------------
// TxOutput database helpers
// -------------------------

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
