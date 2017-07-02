package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
)

func TestDBBlockMapper(t *testing.T) {
	tx := &Transaction{TableName: []byte("cars")}
	b := &Block{
		Transactions: []*Transaction{tx},
		CreatedAt:    big.NewInt(43),
		Creator:      []byte{44},
		Voters:       [][]byte{[]byte{45}},
		State:        BLOCK_STATE_ACCEPTED,
	}
	hash := rlpHash(&blockBody{
		Transactions: []Hash{tx.Hash()},
		Voters:       b.Voters,
	})
	txHash := rlpHash(&transactionBody{TableName: tx.TableName})

	expected := &meddb.Block{
		Hash: hash.Bytes(),
		Transactions: []*meddb.Transaction{&meddb.Transaction{
			Hash:      txHash.Bytes(),
			TableName: []byte("cars"),
		}},
		CreatedAt: big.NewInt(43),
		Creator:   []byte{44},
		Voters:    [][]byte{[]byte{45}},
		State:     1,
	}
	actual := b.toDBBlock()
	assert.Equal(t, expected, actual)

	back := fromDBBlock(actual)
	assert.Equal(t, b, back)
}

func TestDBBlockMapperEmpty(t *testing.T) {
	b := &Block{}
	hash := rlpHash(&blockBody{})

	expected := &meddb.Block{
		Hash: hash.Bytes(),
	}
	actual := b.toDBBlock()
	assert.Equal(t, expected, actual)

	back := fromDBBlock(actual)
	assert.Equal(t, b, back)
}
