package ledger

import (
	"bytes"
	"encoding/binary"
	"math/big"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/wojtechnology/glacier/meddb"
)

type BlockHeader struct {
	ParentHash Hash
	Number     *big.Int
	Dt         *big.Int
	Nonce      BlockNonce
}

type BlockBody struct {
	Transactions []*Transaction
}

type Block struct {
	Header *BlockHeader
	Body   *BlockBody
}

// ----------------------
// Block database helpers
// ----------------------

// Returns big-endian encoded block number for header
func encodeBlockHeaderNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

// Concatenates elements to build key for storing block parts in database
func buildBlockHeaderKey(prefix string, hash Hash, number uint64) []byte {
	encNum := encodeBlockHeaderNumber(number)
	return append(append([]byte(prefix), hash.Bytes()...), encNum...)
}

// ----------------
// Block Header API
// ----------------

// Returns Sha256 hash of block header in rlp format
func (h *BlockHeader) Hash() Hash {
	// TODO: Cache this value
	return rlpHash(h)
}

// Writes block header to provided database in rlp format
func (h *BlockHeader) Write(db meddb.Database) error {
	key := buildBlockHeaderKey(blockHeaderPrefix, h.Hash(), h.Number.Uint64())
	return writeRlp(db, key, h)
}

// Writes head block header to provided database in rlp format
func (h *BlockHeader) WriteHead(db meddb.Database) error {
	key := []byte(headKey)
	return writeRlp(db, key, h)
}

// Gets block header from database in rlp format, constructs object and returns
func GetBlockHeader(db meddb.Database, hash Hash, number uint64) (*BlockHeader, error) {
	// TODO: Think about the error handling in this function
	key := buildBlockHeaderKey(blockHeaderPrefix, hash, number)
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

// Gets head block header from database in rlp format, constructs objects and returns
func GetHeadBlockHeader(db meddb.Database) (*BlockHeader, error) {
	// TODO: Think about the error handling in this function
	key := []byte(headKey)
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

// --------------
// Block Body API
// --------------

// Writes block to provided database in rlp format
func (b *BlockBody) Write(db meddb.Database, hash Hash, number uint64) error {
	key := buildBlockHeaderKey(blockBodyPrefix, hash, number)
	return writeRlp(db, key, b)
}

func GetBlockBody(db meddb.Database, hash Hash, number uint64) (*BlockBody, error) {
	// TODO: think about the error handling in this function
	key := buildBlockHeaderKey(blockBodyPrefix, hash, number)
	data, err := db.Get(key)
	if err != nil {
		return nil, err
	}

	b := new(BlockBody)
	if err = rlp.Decode(bytes.NewReader(data), b); err != nil {
		return nil, err
	}
	return b, nil
}

// ---------
// Block API
// ---------

func (b *Block) writeBody(db meddb.Database) error {
	err := b.Body.Write(db, b.Header.Hash(), b.Header.Number.Uint64())
	if err != nil {
		return err
	}
	return nil
}

// Writes block header and body to the database
func (b *Block) Write(db meddb.Database) error {
	err := b.Header.Write(db)
	if err != nil {
		return err
	}
	err = b.writeBody(db)
	if err != nil {
		return err
	}
	return nil
}

// Writes block header and body to the database using the head block key
func (b *Block) WriteHead(db meddb.Database) error {
	err := b.Header.WriteHead(db)
	if err != nil {
		return err
	}
	err = b.writeBody(db)
	if err != nil {
		return err
	}
	return nil
}

// Gets whole block from database
func GetBlock(db meddb.Database, hash Hash, number uint64) (*Block, error) {
	header, err := GetBlockHeader(db, hash, number)
	if err != nil {
		return nil, err
	}

	body, err := GetBlockBody(db, hash, number)
	if err != nil {
		return nil, err
	}

	b := &Block{
		Header: header,
		Body:   body,
	}
	return b, nil
}

// Gets whole head block from database
func GetHeadBlock(db meddb.Database) (*Block, error) {
	header, err := GetHeadBlockHeader(db)
	if err != nil {
		return nil, err
	}

	body, err := GetBlockBody(db, header.Hash(), header.Number.Uint64())
	if err != nil {
		return nil, err
	}

	b := &Block{
		Header: header,
		Body:   body,
	}
	return b, nil
}

// Gets and returns the genesis block if it exists in the database
// Otherwise, creates the genesis block, commits it to the database and returns it
func GetOrCreateGenesisBlock(db meddb.Database) (*Block, error) {
	// TODO: test
	gen := genesis()
	curGen, err := GetBlock(db, gen.Header.Hash(), gen.Header.Number.Uint64())
	if err != nil {
		// Means that we haven't found the genesis block
		err = gen.Write(db)
		if err != nil {
			return nil, err
		}
	} else {
		gen = curGen
	}
	return gen, nil
}

// Creates and returns genesis block
func genesis() *Block {
	header := &BlockHeader{
		ParentHash: StringToHash(genesisParentHash),
		Number:     big.NewInt(0),
		Dt:         big.NewInt(0),
		Nonce:      EncodeNonce(0),
	}
	body := &BlockBody{
		Transactions: make([]*Transaction, 0),
	}
	genesis := &Block{
		Header: header,
		Body:   body,
	}
	return genesis
}
