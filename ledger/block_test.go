package ledger

import (
	"math/big"
	"testing"

	"github.com/wojtechnology/medblocks/meddb"
	"github.com/wojtechnology/medblocks/test"
)

func TestWriteReadBlockHeader(t *testing.T) {
	h := &BlockHeader{
		ParentHash: StringToHash("the parent is here yo"),
		Number:     big.NewInt(42),
		Dt:         big.NewInt(43),
		Nonce:      EncodeNonce(44),
	}
	db, _ := meddb.NewMemoryDatabase()

	err := h.Write(db)
	test.AssertEqual(t, nil, err)

	var newH *BlockHeader
	newH, err = GetBlockHeader(db, h.Hash(), h.Number.Uint64())

	test.AssertEqual(t, nil, err)
	test.AssertEqual(t, h, newH)
}

func TestWriteReadHeadBlockHeader(t *testing.T) {
	h := &BlockHeader{
		ParentHash: StringToHash("the parent is here yo"),
		Number:     big.NewInt(42),
		Dt:         big.NewInt(43),
		Nonce:      EncodeNonce(44),
	}
	db, _ := meddb.NewMemoryDatabase()

	err := h.Write(db)
	test.AssertEqual(t, nil, err)

	var newH *BlockHeader
	newH, err = GetBlockHeader(db, h.Hash(), h.Number.Uint64())

	test.AssertEqual(t, nil, err)
	test.AssertEqual(t, h, newH)
}

func TestWriteReadBlockBody(t *testing.T) {
	b := new(BlockBody)
	trans := &Transaction{V: big.NewInt(42), R: big.NewInt(43), S: big.NewInt(44)}
	b.Transactions = append(b.Transactions, trans)
	db, _ := meddb.NewMemoryDatabase()
	hash := StringToHash("some hash")
	var number uint64 = 42

	err := b.Write(db, hash, number)
	test.AssertEqual(t, nil, err)

	var newB *BlockBody
	newB, err = GetBlockBody(db, hash, number)

	test.AssertEqual(t, nil, err)
	test.AssertEqual(t, *b.Transactions[0], *newB.Transactions[0])
}
