package ledger

import (
	"bytes"
	"encoding/binary"
	"golang.org/x/crypto/sha3"
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/wojtechnology/medblocks/meddb"
)

type BlockNonce [8]byte

var (
	blockHeaderPrefix = []byte("h")
	blockBodyPrefix   = []byte("b")
)

type BlockHeader struct {
	ParentHash Hash
	Number     *big.Int
	Dt         *big.Int
	Nonce      BlockNonce
}

type Block struct {
	Header       *BlockHeader
	Transactions []*Transaction
}

// Returns big-endian encoded block number for header
func encodeBlockHeaderNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

func blockHeaderKey(hash Hash, number uint64) []byte {
	encNum := encodeBlockHeaderNumber(number)
	return append(append(blockHeaderPrefix, hash.Bytes()...), encNum...)
}

// Returns Sha256 hash of block header in rlp format
func (h *BlockHeader) Hash() (hash Hash) {
	// TODO: Cache this value
	w := sha3.New256()
	rlp.Encode(w, h)
	w.Sum(hash[:0])
	return hash
}

// Writes block to provided database in rlp format
func (h *BlockHeader) Write(db meddb.Database) error {
	data, err := rlp.EncodeToBytes(h)
	if err != nil {
		return err
	}

	key := blockHeaderKey(h.Hash(), h.Number.Uint64())
	if err = db.Put(key, data); err != nil {
		return err
	}
	return nil
}

// Gets block header from database in rlp format, constructs object and returns
func GetBlockHeader(db meddb.Database, hash Hash, number uint64) (*BlockHeader, error) {
	// TODO: Think about the error handling in this function
	key := blockHeaderKey(hash, number)
	data, err := db.Get(key)
	if err != nil {
		return nil, err
	}

	h := new(BlockHeader)
	if err := rlp.Decode(bytes.NewReader(data), h); err != nil {
		return nil, err
	}
	return h, nil
}

// Creates and returns genesis block
func Genesis() *Block {
	header := &BlockHeader{Number: big.NewInt(0), Dt: big.NewInt(0)}
	genesis := &Block{Header: header}
	return genesis
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
