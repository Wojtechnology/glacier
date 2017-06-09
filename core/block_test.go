package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
)

func TestDBBlockMapper(t *testing.T) {
	b := &Block{
		Transactions: [][]byte{[]byte{42}},
		CreatedAt:    big.NewInt(43),
		Creator:      []byte{44},
	}
	hash := rlpHash(b)

	expected := &meddb.Block{
		Hash:         hash.Bytes(),
		Transactions: [][]byte{[]byte{42}},
		CreatedAt:    big.NewInt(43),
		Creator:      []byte{44},
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
