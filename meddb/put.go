package meddb

import "math/big"

type PutOp struct {
	rowId []byte
	cols  map[string]*Cell
}

func NewPutOp(rowId []byte) *PutOp {
	return &PutOp{rowId: rowId, cols: make(map[string]*Cell)}
}

func (op *PutOp) AddCol(colId []byte, data []byte) error {
	if _, ok := op.cols[string(colId)]; ok {
		return &ColIdAlreadyExists{ColId: colId}
	}
	op.cols[string(colId)] = NewCell(data)
	return nil
}

func (op *PutOp) AddColVer(colId []byte, verId int64, data []byte) error {
	if _, ok := op.cols[string(colId)]; ok {
		return &ColIdAlreadyExists{ColId: colId}
	}
	op.cols[string(colId)] = NewCellVer(verId, data)
	return nil
}

func (op *PutOp) fillVer(verId int64) {
	for _, cell := range op.cols {
		if cell.VerId == nil {
			cell.VerId = big.NewInt(verId)
		}
	}
}
