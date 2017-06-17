package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
)

func TestDBBlockMapper(t *testing.T) {
	tx := &Transaction{Data: []byte{42}}
	b := &Block{
		Transactions: []*Transaction{tx},
		CreatedAt:    big.NewInt(43),
		Creator:      []byte{44},
		Voters:       [][]byte{[]byte{45}},
	}
	hash := rlpHash(b)
	txHash := rlpHash(&transactionBody{CellAddress: tx.CellAddress, Data: tx.Data})

	expected := &meddb.Block{
		Hash: hash.Bytes(),
		Transactions: []*meddb.Transaction{&meddb.Transaction{
			Hash: txHash.Bytes(),
			Data: []byte{42},
		}},
		CreatedAt: big.NewInt(43),
		Creator:   []byte{44},
		Voters:    [][]byte{[]byte{45}},
	}
	actual := b.toDBBlock()
	assert.Equal(t, expected, actual)

	back := fromDBBlock(actual)
	assert.Equal(t, b, back)
}

func TestDBBlockMapperEmpty(t *testing.T) {
	b := &Block{}
	hash := rlpHash(b)

	expected := &meddb.Block{
		Hash: hash.Bytes(),
	}
	actual := b.toDBBlock()
	assert.Equal(t, expected, actual)

	back := fromDBBlock(actual)
	assert.Equal(t, b, back)
}
