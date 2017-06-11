package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
)

func TestAddTransaction(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	pubKey := []byte{69}
	tx := &Transaction{
		CellAddress: &CellAddress{
			TableName: []byte{123},
			RowId:     []byte{124},
			ColId:     []byte{125},
			VerId:     big.NewInt(126),
		},
		Data: []byte{127},
	}

	bc := NewBlockchain(db, nil, []*Node{&Node{PubKey: pubKey}})

	err = bc.AddTransaction(tx)
	assert.Nil(t, err)
	assert.Equal(t, pubKey, tx.AssignedTo)
	assert.NotNil(t, tx.AssignedAt)

	txs, err := db.GetAssignedTransactions(pubKey)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, tx, fromDBTransaction(txs[0]))
}

func TestRandomAssignee(t *testing.T) {
	node := &Node{PubKey: []byte{42}}
	otherNode := &Node{PubKey: []byte{43}}

	bc := &Blockchain{federation: []*Node{node, otherNode}}
	randNode := bc.randomAssignee(0)
	assert.Equal(t, node, randNode)
	randNode = bc.randomAssignee(1)
	assert.Equal(t, otherNode, randNode)
}
