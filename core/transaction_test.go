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
		Outputs: []Output{&TableExistsOutput{[]byte{0}}, &TableExistsOutput{[]byte{1}}},
		Inputs:  []Input{&AdminInput{InputLink{}, []byte{2}}, &AdminInput{InputLink{}, []byte{3}}},
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
		OutputHashes: [][]byte{
			hashOutput(tx.Outputs[0]).Bytes(),
			hashOutput(tx.Outputs[1]).Bytes(),
		},
		InputHashes: [][]byte{
			hashInput(tx.Inputs[0]).Bytes(),
			hashInput(tx.Inputs[1]).Bytes(),
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
		Outputs: []Output{&TableExistsOutput{[]byte{0}}, &TableExistsOutput{[]byte{1}}},
		Inputs:  []Input{&AdminInput{InputLink{}, []byte{2}}, &AdminInput{InputLink{}, []byte{3}}},
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
		OutputHashes: [][]byte{
			hashOutput(tx.Outputs[0]).Bytes(),
			hashOutput(tx.Outputs[1]).Bytes(),
		},
		InputHashes: [][]byte{
			hashInput(tx.Inputs[0]).Bytes(),
			hashInput(tx.Inputs[1]).Bytes(),
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
		Outputs: []*meddb.Output{
			&meddb.Output{
				Hash: hashOutput(tx.Outputs[0]).Bytes(),
				Type: int(tx.Outputs[0].Type()),
				Data: tx.Outputs[0].Data(),
			},
			&meddb.Output{
				Hash: hashOutput(tx.Outputs[1]).Bytes(),
				Type: int(tx.Outputs[1].Type()),
				Data: tx.Outputs[1].Data(),
			},
		},
		Inputs: []*meddb.Input{
			&meddb.Input{
				OutputHash: tx.Inputs[0].OutputHash().Bytes(),
				Type:       int(tx.Inputs[0].Type()),
				Data:       tx.Inputs[0].Data(),
			},
			&meddb.Input{
				OutputHash: tx.Inputs[1].OutputHash().Bytes(),
				Type:       int(tx.Inputs[1].Type()),
				Data:       tx.Inputs[1].Data(),
			},
		},
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
