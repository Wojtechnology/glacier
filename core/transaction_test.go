package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
)

func TestTransactionHash(t *testing.T) {
	tx := &Transaction{
		AssignedTo: []byte{12},
		AssignedAt: big.NewInt(420),
		TableName:  []byte{123},
		RowId:      []byte{124},
		Cols: map[string]*Cell{
			string([]byte{125}): &Cell{
				VerId: big.NewInt(69),
				Data:  []byte{70},
			},
			string([]byte{69}): &Cell{
				VerId: big.NewInt(126),
				Data:  []byte{127},
			},
		},
	}

	expected := rlpHash(&transactionBody{
		TableName: tx.TableName,
		RowId:     tx.RowId,
		Cols: []*colCell{
			&colCell{
				ColId: []byte{69},
				Cell: &Cell{
					VerId: big.NewInt(126),
					Data:  []byte{127},
				},
			},
			&colCell{
				ColId: []byte{125},
				Cell: &Cell{
					VerId: big.NewInt(69),
					Data:  []byte{70},
				},
			},
		},
	})
	assert.Equal(t, expected, tx.Hash())
}

func TestDBTransactionMapper(t *testing.T) {
	tx := &Transaction{
		AssignedTo: []byte{12},
		AssignedAt: big.NewInt(420),
		TableName:  []byte{123},
		RowId:      []byte{124},
		Cols: map[string]*Cell{
			string([]byte{125}): &Cell{
				VerId: big.NewInt(126),
				Data:  []byte{127},
			},
		},
	}
	hash := rlpHash(&transactionBody{
		TableName: tx.TableName,
		RowId:     tx.RowId,
		Cols: []*colCell{
			&colCell{
				ColId: []byte{125},
				Cell: &Cell{
					VerId: big.NewInt(126),
					Data:  []byte{127},
				},
			},
		},
	})

	expected := &meddb.Transaction{
		Hash:      hash.Bytes(),
		TableName: []byte{123},
		RowId:     []byte{124},
		Cols: map[string]*meddb.Cell{
			string([]byte{125}): &meddb.Cell{
				VerId: big.NewInt(126),
				Data:  []byte{127},
			},
		},
		AssignedTo: []byte{12},
		AssignedAt: big.NewInt(420),
	}
	actual := tx.toDBTransaction()
	assert.Equal(t, expected, actual)

	back := fromDBTransaction(actual)
	assert.Equal(t, tx, back)
}

func TestDBTransactionMapperEmpty(t *testing.T) {
	tx := &Transaction{}
	hash := rlpHash(&transactionBody{})

	expected := &meddb.Transaction{Hash: hash.Bytes()}
	actual := tx.toDBTransaction()
	assert.Equal(t, expected, actual)

	back := fromDBTransaction(actual)
	assert.Equal(t, tx, back)
}
