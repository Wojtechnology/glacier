package core

import (
	"golang.org/x/crypto/sha3"

	"github.com/ethereum/go-ethereum/rlp"
)

// Returns hash of rlp encoded object
func rlpHash(o interface{}) (hash Hash) {
	w := sha3.New256()
	rlp.Encode(w, o)
	w.Sum(hash[:0])
	return hash
}
