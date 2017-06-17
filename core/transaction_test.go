package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
)

func TestDBTransactionMapper(t *testing.T) {
	tx := &Transaction{
		AssignedTo: []byte{12},
		AssignedAt: big.NewInt(420),
		CellAddress: &CellAddress{
			TableName: []byte{42},
			RowId:     []byte{32},
			ColId:     []byte{43},
			VerId:     big.NewInt(4),
		},
		Data: []byte{69},
	}
	hash := rlpHash(&transactionBody{CellAddress: tx.CellAddress, Data: tx.Data})

	expected := &meddb.Transaction{
		Hash: hash.Bytes(),
		CellAddress: &meddb.CellAddress{
			TableName: []byte{42},
			RowId:     []byte{32},
			ColId:     []byte{43},
			VerId:     big.NewInt(4),
		},
		AssignedTo: []byte{12},
		AssignedAt: big.NewInt(420),
		Data:       []byte{69},
	}
	actual := tx.toDBTransaction()
	assert.Equal(t, expected, actual)

	back := fromDBTransaction(actual)
	assert.Equal(t, tx, back)
}

func TestDBTransactionMapperEmpty(t *testing.T) {
	tx := &Transaction{
		CellAddress: &CellAddress{},
	}
	hash := rlpHash(&transactionBody{CellAddress: tx.CellAddress, Data: tx.Data})

	expected := &meddb.Transaction{
		Hash:        hash.Bytes(),
		CellAddress: &meddb.CellAddress{},
	}
	actual := tx.toDBTransaction()
	assert.Equal(t, expected, actual)

	back := fromDBTransaction(actual)
	assert.Equal(t, tx, back)
}
