package meddb

import "math/big"

type Cell struct {
	VerId *big.Int
	Data  []byte
}

func NewCell(data []byte) *Cell {
	return &Cell{Data: data}
}

func NewCellVer(verId int64, data []byte) *Cell {
	return &Cell{VerId: big.NewInt(verId), Data: data}
}

func (c *Cell) Clone() *Cell {
	// TODO(wojtek): check if need to copy all of the []byte
	if c.VerId != nil {
		return NewCellVer(c.VerId.Int64(), c.Data)
	}
	return NewCell(c.Data)
}
