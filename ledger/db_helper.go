package ledger

import (
	"golang.org/x/crypto/sha3"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/wojtechnology/glacier/meddb"
)

// Writes an object to database in rlp format
func writeRlp(db meddb.Database, key []byte, obj interface{}) error {
	data, err := rlp.EncodeToBytes(obj)
	if err != nil {
		return err
	}

	if err = db.Put(key, data); err != nil {
		return err
	}
	return nil
}

// Returns hash of rlp encoded object
func rlpHash(o interface{}) (hash Hash) {
	w := sha3.New256()
	rlp.Encode(w, o)
	w.Sum(hash[:0])
	return hash
}
