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
	TableName []byte
	RowId     []byte
	Cols      []*colCell
}

func (tx *Transaction) Hash() Hash {
	var cols []*colCell = nil
	if tx.Cols != nil {
		cols = make([]*colCell, len(tx.Cols))
		i := 0
		for colId, cell := range tx.Cols {
			cols[i] = &colCell{ColId: []byte(colId), Cell: cell}
			i++
		}
	}
	sort.Slice(cols, func(i, j int) bool {
		return string(cols[i].ColId) < string(cols[j].ColId)
	})
	return rlpHash(&transactionBody{TableName: tx.TableName, RowId: tx.RowId, Cols: cols})
}

func (tx *Transaction) Validate() bool {
	return true
}

func (tx *Transaction) toDBTransaction() *meddb.Transaction {
	var lastAssigned *big.Int = nil
	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}
	var cols map[string]*meddb.Cell = nil
	if tx.Cols != nil {
		cols = make(map[string]*meddb.Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = toDBCell(cell)
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
	}
}

func fromDBTransaction(tx *meddb.Transaction) *Transaction {
	var lastAssigned *big.Int = nil
	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}
	var cols map[string]*Cell = nil
	if tx.Cols != nil {
		cols = make(map[string]*Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = fromDBCell(cell)
		}
	}

	// TODO(wojtek): Maybe make copies here
	return &Transaction{
		AssignedTo: tx.AssignedTo,
		AssignedAt: lastAssigned,
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
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
