package core

import (
	"math/big"

	"github.com/wojtechnology/glacier/meddb"
)

type CellAddress struct {
	TableName []byte
	RowId     []byte
	ColId     []byte
	VerId     *big.Int
}

type Transaction struct {
	AssignedTo  []byte // TODO: Make more strict type for public keys
	AssignedAt  *big.Int
	CellAddress *CellAddress
	Data        []byte
}

// ---------------
// Transaction API
// ---------------

// Part of transaction used in hash
type transactionBody struct {
	CellAddress *CellAddress
	Data        []byte
}

func (tx *Transaction) Hash() Hash {
	return rlpHash(&transactionBody{CellAddress: tx.CellAddress, Data: tx.Data})
}

func (tx *Transaction) Valid() bool {
	return true
}

func (tx *Transaction) toDBTransaction() *meddb.Transaction {
	var lastAssigned *big.Int = nil
	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}

	var cellAddress *meddb.CellAddress = nil
	if tx.CellAddress != nil {
		cellAddress = tx.CellAddress.toDBCellAddress()
	}
	// TODO(wojtek): Maybe make copies here
	return &meddb.Transaction{
		Hash:        tx.Hash().Bytes(),
		AssignedTo:  tx.AssignedTo,
		AssignedAt:  lastAssigned,
		CellAddress: cellAddress,
		Data:        tx.Data,
	}
}

func fromDBTransaction(tx *meddb.Transaction) *Transaction {
	var lastAssigned *big.Int = nil
	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}

	var cellAddress *CellAddress = nil
	if tx.CellAddress != nil {
		cellAddress = fromDBCellAddress(tx.CellAddress)
	}
	// TODO(wojtek): Maybe make copies here
	return &Transaction{
		AssignedTo:  tx.AssignedTo,
		AssignedAt:  lastAssigned,
		CellAddress: cellAddress,
		Data:        tx.Data,
	}
}

func fromDBTransactions(dbTxs []*meddb.Transaction) []*Transaction {
	txs := make([]*Transaction, len(dbTxs))
	for i, dbTx := range dbTxs {
		txs[i] = fromDBTransaction(dbTx)
	}
	return txs
}

// ---------------
// CellAddress API
// ---------------

func (ca *CellAddress) toDBCellAddress() *meddb.CellAddress {
	var verId *big.Int = nil
	if ca.VerId != nil {
		verId = big.NewInt(ca.VerId.Int64())
	}

	return &meddb.CellAddress{
		TableName: ca.TableName,
		RowId:     ca.RowId,
		ColId:     ca.ColId,
		VerId:     verId,
	}
}

func fromDBCellAddress(ca *meddb.CellAddress) *CellAddress {
	var verId *big.Int = nil
	if ca.VerId != nil {
		verId = big.NewInt(ca.VerId.Int64())
	}

	return &CellAddress{
		TableName: ca.TableName,
		RowId:     ca.RowId,
		ColId:     ca.ColId,
		VerId:     verId,
	}
}
