package core

import (
	"math/big"
	"sort"

	"github.com/wojtechnology/glacier/meddb"
)

type Cell struct {
	Data  []byte
	VerId *big.Int
}

type Transaction struct {
	AssignedTo []byte // TODO: Make more strict type for public keys
	AssignedAt *big.Int
	TableName  []byte
	RowId      []byte
	Cols       map[string]*Cell
	Outputs    []Output
	Inputs     []Input
}

// ---------------
// Transaction API
// ---------------

type colCell struct {
	ColId []byte
	Cell  *Cell
}

// Part of transaction used in hash
type transactionBody struct {
	TableName    []byte
	RowId        []byte
	Cols         []*colCell
	OutputHashes [][]byte
	InputHashes  [][]byte
}

func (tx *Transaction) Hash() Hash {
	var (
		cols         []*colCell = nil
		outputHashes [][]byte   = nil
		inputHashes  [][]byte   = nil
	)

	if tx.Cols != nil {
		cols = make([]*colCell, len(tx.Cols))
		i := 0
		for colId, cell := range tx.Cols {
			cols[i] = &colCell{ColId: []byte(colId), Cell: cell}
			i++
		}

		// Sorting makes the hash deterministic
		sort.Slice(cols, func(i, j int) bool {
			return string(cols[i].ColId) < string(cols[j].ColId)
		})
	}

	if tx.Outputs != nil {
		outputHashes = make([][]byte, len(tx.Outputs))
		for i, output := range tx.Outputs {
			outputHashes[i] = hashOutput(output).Bytes()
		}
	}

	if tx.Inputs != nil {
		outputHashes = make([][]byte, len(tx.Inputs))
		for i, input := range tx.Inputs {
			inputHashes[i] = hashInput(input).Bytes()
		}
	}

	return rlpHash(&transactionBody{
		TableName:    tx.TableName,
		RowId:        tx.RowId,
		Cols:         cols,
		OutputHashes: outputHashes,
		InputHashes:  inputHashes,
	})
}

func (tx *Transaction) Validate() bool {
	return true
}

func (tx *Transaction) toDBTransaction() *meddb.Transaction {
	var (
		lastAssigned *big.Int               = nil
		cols         map[string]*meddb.Cell = nil
		outputs      []*meddb.Output        = nil
		inputs       []*meddb.Input         = nil
	)

	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}

	if tx.Cols != nil {
		cols = make(map[string]*meddb.Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = toDBCell(cell)
		}
	}

	if tx.Outputs != nil {
		outputs = make([]*meddb.Output, len(tx.Outputs))
		for i, output := range tx.Outputs {
			outputs[i] = toDBOutput(output)
		}
	}

	if tx.Inputs != nil {
		inputs = make([]*meddb.Input, len(tx.Inputs))
		for i, input := range tx.Inputs {
			inputs[i] = toDBInput(input)
		}
	}

	// TODO(wojtek): Maybe make copies here
	return &meddb.Transaction{
		Hash:       tx.Hash().Bytes(),
		AssignedTo: tx.AssignedTo,
		AssignedAt: lastAssigned,
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
		Outputs:    outputs,
		Inputs:     inputs,
	}
}

func fromDBTransaction(tx *meddb.Transaction) *Transaction {
	var (
		lastAssigned *big.Int         = nil
		cols         map[string]*Cell = nil
		outputs      []Output         = nil
		inputs       []Input          = nil
	)

	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}

	if tx.Cols != nil {
		cols = make(map[string]*Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = fromDBCell(cell)
		}
	}

	if tx.Outputs != nil {
		outputs = make([]Output, len(tx.Outputs))
		for i, output := range tx.Outputs {
			// TODO: Log when error occurs, since this should not be able to error
			outputs[i], _ = fromDBOutput(output)
		}
	}

	if tx.Inputs != nil {
		inputs = make([]Input, len(tx.Inputs))
		for i, input := range tx.Inputs {
			// TODO: Log when error occurs, since this should not be able to error
			inputs[i], _ = fromDBInput(input)
		}
	}

	// TODO(wojtek): Maybe make copies here
	return &Transaction{
		AssignedTo: tx.AssignedTo,
		AssignedAt: lastAssigned,
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
		Outputs:    outputs,
		Inputs:     inputs,
	}
}

func fromDBTransactions(dbTxs []*meddb.Transaction) []*Transaction {
	txs := make([]*Transaction, len(dbTxs))
	for i, dbTx := range dbTxs {
		txs[i] = fromDBTransaction(dbTx)
	}
	return txs
}

// --------
// Cell API
// --------

func toDBCell(cell *Cell) *meddb.Cell {
	var verId *big.Int = nil
	if cell.VerId != nil {
		verId = big.NewInt(cell.VerId.Int64())
	}
	return &meddb.Cell{
		Data:  cell.Data,
		VerId: verId,
	}
}

func fromDBCell(cell *meddb.Cell) *Cell {
	var verId *big.Int = nil
	if cell.VerId != nil {
		verId = big.NewInt(cell.VerId.Int64())
	}
	return &Cell{
		Data:  cell.Data,
		VerId: verId,
	}
}
