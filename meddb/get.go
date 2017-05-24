package meddb

import "math/big"

type GetOp struct {
	rowId          []byte
	colIds         [][]byte
	limit          uint32
	minVer, maxVer *big.Int
	verId          *big.Int
}

func NewGetOp(rowId []byte, colIds [][]byte) *GetOp {
	return &GetOp{rowId: rowId, colIds: colIds}
}

func NewGetOpLimit(rowId []byte, colIds [][]byte, limit uint32) *GetOp {
	op := NewGetOp(rowId, colIds)
	op.limit = limit
	return op
}

func NewGetOpRange(rowId []byte, colIds [][]byte, minVer, maxVer int64) *GetOp {
	op := NewGetOp(rowId, colIds)
	op.minVer = big.NewInt(minVer)
	op.maxVer = big.NewInt(maxVer)
	return op
}

func NewGetOpVer(rowId []byte, colIds [][]byte, verId int64) *GetOp {
	op := NewGetOp(rowId, colIds)
	op.verId = big.NewInt(verId)
	return op
}
