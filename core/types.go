package core

import "math/big"

const HashLength = 32

type Hash [HashLength]byte

// --------
// Hash API
// --------

func BytesToHash(b []byte) Hash {
	var hash Hash
	hash.SetBytes(b)
	return hash
}

func StringToHash(s string) Hash {
	return BytesToHash([]byte(s))
}

func BigToHash(i *big.Int) Hash {
	return BytesToHash(i.Bytes())
}

func (hash Hash) Bytes() []byte {
	return hash[:]
}

func (hash Hash) String() string {
	return string(hash[:])
}

func (hash Hash) Big() *big.Int {
	return BytesToBig(hash[:])
}

func (hash *Hash) SetBytes(b []byte) {
	if len(b) > len(hash) {
		b = b[len(b)-HashLength:]
	}
	copy(hash[HashLength-len(b):], b)
}

// -------
// Helpers
// -------

func BytesToBig(b []byte) *big.Int {
	i := new(big.Int)
	i.SetBytes(b)
	return i
}
