package ledger

import (
	"encoding/binary"
	"math/big"
)

type BlockNonce [8]byte

type Header struct {
	ParentHash      Hash
	TransactionHash Hash
	Number          *big.Int
	Dt              *big.Int
	Nonce           BlockNonce
}

type Block struct {
	Header       *Header
	Transactions []*Transaction
}

/*
* BlockNonce helper methods
* TODO: Write tests
 */
func EncodeNonce(i uint64) BlockNonce {
	var nonce BlockNonce
	binary.BigEndian.PutUint64(nonce[:], i)
	return nonce
}

func (nonce BlockNonce) Uint64() uint64 {
	return binary.BigEndian.Uint64(nonce[:])
}
