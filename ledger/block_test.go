package ledger

import (
	"math/big"
	"testing"

	"github.com/wojtechnology/medblocks/meddb"
	"github.com/wojtechnology/medblocks/test"
)

func TestEncodeDecodeNonce(t *testing.T) {
	var i uint64 = 42
	test.AssertEqual(t, i, EncodeNonce(i).Uint64())
}

func TestWriteReadBlockHeader(t *testing.T) {
	h := &BlockHeader{
		ParentHash: StringToHash("the parent is here yo"),
		Number:     big.NewInt(42),
		Dt:         big.NewInt(43),
		Nonce:      EncodeNonce(44),
	}
	db, _ := meddb.NewMemoryDatabase()

	h.Write(db)
	newH, _ := GetBlockHeader(db, h.Hash(), h.Number.Uint64())

	test.AssertEqual(t, h, newH)
}
