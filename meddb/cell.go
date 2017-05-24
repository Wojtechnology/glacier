package meddb

import "math/big"

type Cell struct {
	RowId []byte
	ColId []byte
	VerId *big.Int
	Data  []byte
}

func NewCell(rowId, colId, data []byte) *Cell {
	return &Cell{
		RowId: rowId,
		ColId: colId,
		Data:  data,
	}
}

func NewCellVer(rowId, colId []byte, verId int64, data []byte) *Cell {
	return &Cell{
		RowId: rowId,
		ColId: colId,
		VerId: big.NewInt(verId),
		Data:  data,
	}
}

func (c *Cell) Clone() *Cell {
	// TODO(wojtek): check if need to copy all of the []byte
	if c.VerId != nil {
		return NewCellVer(c.RowId, c.ColId, c.VerId.Int64(), c.Data)
	}
	return NewCell(c.RowId, c.ColId, c.Data)
}
