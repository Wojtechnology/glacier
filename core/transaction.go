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
	// TODO(wojtek): signature
	CellAddress *CellAddress
	Data        []byte
}

// ---------------
// Transaction API
// ---------------

func (tx *Transaction) Hash() Hash {
	return rlpHash(tx)
}

func (tx *Transaction) Valid() bool {
	return true
}

func (tx *Transaction) toDBTransaction(
	assignedTo []byte, lastAssigned *big.Int) *meddb.Transaction {
	var cellAddress *meddb.CellAddress = nil
	if tx.CellAddress != nil {
		cellAddress = tx.CellAddress.toDBCellAddress()
	}
	// TODO(wojtek): Maybe make copies here
	return &meddb.Transaction{
		Hash:         tx.Hash().Bytes(),
		AssignedTo:   assignedTo,
		LastAssigned: lastAssigned,
		CellAddress:  cellAddress,
		Data:         tx.Data,
	}
}

func fromDBTransaction(tx *meddb.Transaction) *Transaction {
	var cellAddress *CellAddress = nil
	if tx.CellAddress != nil {
		cellAddress = fromDBCellAddress(tx.CellAddress)
	}
	// TODO(wojtek): Maybe make copies here
	return &Transaction{
		CellAddress: cellAddress,
		Data:        tx.Data,
	}
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
