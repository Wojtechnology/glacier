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
		Hash:         []byte{32},
		AssignedTo:   []byte{42},
		LastAssigned: big.NewInt(123),
		CellAddress: &CellAddress{
			TableName: []byte{52},
			RowId:     []byte{62},
			ColId:     []byte{72},
			VerId:     big.NewInt(234),
		},
		Data: []byte{82},
	}
}
