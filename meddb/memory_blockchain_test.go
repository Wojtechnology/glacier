package meddb

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

// -----------------------
// Test MemoryBlockchainDB
// -----------------------

func TestMemoryWriteTransaction(t *testing.T) {
	db := getMemoryDB(t)
	tx := getTestTransaction()

	err := db.WriteTransaction(tx)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(db.backlogTable))
	assert.Equal(t, tx, db.backlogTable[string(tx.Hash)])
}

func TestMemoryGetAssignedTransactions(t *testing.T) {
	db := getMemoryDB(t)
	pubKey := []byte{69}
	tx := getTestTransaction()
	otherTx := getTestTransaction()
	otherTx.Hash = []byte{22}
	otherTx.AssignedTo = pubKey

	db.backlogTable[string(tx.Hash)] = tx.Clone()
	db.backlogTable[string(otherTx.Hash)] = otherTx.Clone()

	txs, err := db.GetAssignedTransactions(pubKey)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, otherTx, txs[0])
}

func TestMemoryDeleteTransactions(t *testing.T) {
	db := getMemoryDB(t)
	tx := getTestTransaction()
	otherTx := getTestTransaction()
	otherTx.Hash = []byte{22}

	db.backlogTable[string(tx.Hash)] = tx.Clone()
	db.backlogTable[string(otherTx.Hash)] = otherTx.Clone()

	err := db.DeleteTransactions([]*Transaction{tx})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(db.backlogTable))
	_, ok := db.backlogTable[string(tx.Hash)]
	assert.False(t, ok)
}

func TestMemoryWriteBlock(t *testing.T) {
	db := getMemoryDB(t)
	b := getTestBlock()

	err := db.WriteBlock(b)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(db.blockTable))
	assert.Equal(t, b, db.blockTable[string(b.Hash)])
}

func TestMemoryWriteVote(t *testing.T) {
	db := getMemoryDB(t)
	v := getTestVote()

	err := db.WriteVote(v)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(db.voteTable))
	assert.Equal(t, v, db.voteTable[string(v.Hash)])
}

// -------
// Helpers
// -------

func getMemoryDB(t *testing.T) *MemoryBlockchainDB {
	db, err := NewMemoryBlockchainDB()
	assert.Nil(t, err)
	return db
}

func getTestTransaction() *Transaction {
	return &Transaction{
		Hash:       []byte{32},
		AssignedTo: []byte{42},
		AssignedAt: big.NewInt(123),
		CellAddress: &CellAddress{
			TableName: []byte{52},
			RowId:     []byte{62},
			ColId:     []byte{72},
			VerId:     big.NewInt(234),
		},
		Data: []byte{82},
	}
}

func getTestBlock() *Block {
	return &Block{
		Hash:         []byte{132},
		Transactions: []*Transaction{getTestTransaction()},
		CreatedAt:    big.NewInt(162),
		Creator:      []byte{172},
		Voters:       [][]byte{[]byte{182}},
	}
}

func getTestVote() *Vote {
	return &Vote{
		Hash:      []byte{202},
		Voter:     []byte{212},
		VotedAt:   big.NewInt(222),
		PrevBlock: []byte{232},
		NextBlock: []byte{242},
		Value:     true,
	}
}
