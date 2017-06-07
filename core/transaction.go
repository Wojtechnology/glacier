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

func (tx *Transaction) toDBTransaction(
	assignedTo []byte, lastAssigned *big.Int) *meddb.Transaction {
	// TODO(wojtek): Maybe make copies here
	return &meddb.Transaction{
		Hash:         tx.Hash().Bytes(),
		AssignedTo:   assignedTo,
		LastAssigned: lastAssigned,
		CellAddress:  tx.CellAddress.toDBCellAddress(),
		Data:         tx.Data,
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
