package ledger

import (
	"encoding/binary"
	"math/big"

	"github.com/wojtechnology/glacier/crypto"
)

const (
	HashLength    = 32
	AddressLength = 20
)

type (
	Hash         [HashLength]byte
	Address      [AddressLength]byte
	BlockNonce   [8]byte
	AccountNonce BlockNonce
)

/*
* Hash helper methods
 */
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

/*
* Address helper methods
 */
func BytesToAddress(b []byte) Address {
	var address Address
	address.SetBytes(b)
	return address
}

func StringToAddress(s string) Address {
	return BytesToAddress([]byte(s))
}

func BigToAddress(i *big.Int) Address {
	return BytesToAddress(i.Bytes())
}

func (address Address) Bytes() []byte {
	return address[:]
}

func (address Address) String() string {
	return string(address[:])
}

func (address Address) Big() *big.Int {
	return BytesToBig(address[:])
}

func (address *Address) SetBytes(b []byte) {
	if len(b) > len(address) {
		b = b[len(b)-AddressLength:]
	}
	copy(address[AddressLength-len(b):], b)
}

// Uses HASH160 (SHA256 + RIPEMD160) to generate the address from given public key
// Makes no assumption about whether this is a compressed or uncompressed public key
func AddressFromPubKey(pub []byte) Address {
	return crypto.Hash160(pub)
}

/*
* Other helpers
 */
func BytesToBig(b []byte) *big.Int {
	i := new(big.Int)
	i.SetBytes(b)
	return i
}

// Returns byte representation of big int, left padded with 0's to be as long as n
// If length of x is larger than n, just returns n
func PaddedBytes(x *big.Int, n int) []byte {
	data := x.Bytes()
	if len(data) >= n {
		return data
	}
	padded := make([]byte, n)
	diff := n - len(data)
	for i := 0; i < diff; i++ {
		padded[i] = 0
	}
	for i := diff; i < len(padded); i++ {
		padded[i] = data[i-diff]
	}
	return padded
}

/*
* BlockNonce helper methods
 */
func EncodeNonce(i uint64) BlockNonce {
	var nonce BlockNonce
	binary.BigEndian.PutUint64(nonce[:], i)
	return nonce
}

func (nonce BlockNonce) Uint64() uint64 {
	return binary.BigEndian.Uint64(nonce[:])
}
