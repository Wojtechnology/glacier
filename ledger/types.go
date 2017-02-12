package ledger

import "math/big"

const (
	HashLength    = 32
	AddressLength = 20
)

type (
	Hash    [HashLength]byte
	Address [AddressLength]byte
)

/*
* Hash helper methods
* TODO: Write tests
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
* TODO: Write tests
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

/*
* Other helpers
 */
func BytesToBig(b []byte) *big.Int {
	i := new(big.Int)
	i.SetBytes(b)
	return i
}
