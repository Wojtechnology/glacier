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
