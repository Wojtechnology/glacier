package core

import (
	"bytes"
	"golang.org/x/crypto/sha3"
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"
)

// Returns hash of rlp encoded object
func rlpHash(o interface{}) (hash Hash) {
	w := sha3.New256()
	rlp.Encode(w, o)
	w.Sum(hash[:0])
	return hash
}

// Returns object as an rlp encoded byte string
func rlpEncode(o interface{}) ([]byte, error) {
	return rlp.EncodeToBytes(o)
}

// Parses rlp encoded byte string into an object
func rlpDecode(b []byte, o interface{}) error {
	return rlp.Decode(bytes.NewReader(b), o)
}

func intToBigInt(x int) *big.Int {
	return big.NewInt(int64(x))
}

func bigIntToInt(x *big.Int) int {
	return int(x.Int64())
}
