package meddb

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"
)

type Bigtable interface {
	Put(tableName, rowId []byte, cells []*Cell) error
	Get(tableName, rowId []byte, cells []*Cell) ([]*Cell, error)
	CreateTable(tableName []byte) error
}

type Cell struct {
	ColId []byte
	VerId *big.Int
	Data  []byte
}

type MemoryBigtable struct {
	tables map[string]*memoryTable
	lock   sync.RWMutex // TODO(wojtek): Optimize by having more granular mutexes
}

type memoryTable struct {
	rows map[string]*memoryRow
}

type memoryRow struct {
	cols map[string]*memoryCol
}

type memoryCol struct {
	cells        map[string]*Cell
	largestVerId *big.Int
}

func NewCell(colId []byte, verId *big.Int, data []byte) *Cell {
	return &Cell{
		ColId: colId,
		VerId: verId,
		Data:  data,
	}
}

func (c *Cell) Clone() *Cell {
	var verIdCopy *big.Int = nil
	if c.VerId != nil {
		verIdCopy = new(big.Int)
		verIdCopy = verIdCopy.Set(c.VerId)
	}
	copyCell := NewCell(c.ColId, verIdCopy, c.Data)
	return copyCell
}

func (table *memoryTable) getRow(rowId []byte) (*memoryRow, error) {
	row, ok := table.rows[string(rowId)]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Row \"%v\" does not exist\n", rowId))
	}
	return row, nil
}

func (row *memoryRow) getCol(colId []byte) (*memoryCol, error) {
	col, ok := row.cols[string(colId)]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Col \"%v\" does not exist\n", colId))
	}
	return col, nil
}

func (col *memoryCol) getCell(verId *big.Int) (*Cell, error) {
	cell, ok := col.cells[string(verId.Bytes())]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Ver \"%v\" does not exist\n", verId))
	}
	return cell, nil
}

func NewMemoryBigtable() (*MemoryBigtable, error) {
	return &MemoryBigtable{
		tables: make(map[string]*memoryTable),
	}, nil
}

func (t *MemoryBigtable) getTable(tableName []byte) (*memoryTable, error) {
	table, ok := t.tables[string(tableName)]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Table \"%v\" does not exist\n", tableName))
	}
	return table, nil
}

func (t *MemoryBigtable) Put(tableName, rowId []byte, cells []*Cell) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	table, err := t.getTable(tableName)
	if err != nil {
		return err
	}

	row, err := table.getRow(rowId)
	if err != nil {
		// Row doesn't exist yet. If other errors can be thrown, check type here.
		row = &memoryRow{cols: make(map[string]*memoryCol)}
		table.rows[string(rowId)] = row
	}

	curTimestamp := curTimeMillis()
	cellsToWrite := make([]*Cell, 0)
	for _, cell := range cells {
		cell = cell.Clone()
		if cell.VerId == nil {
			cell.VerId = big.NewInt(int64(curTimestamp))
		}

		col, err := row.getCol(cell.ColId)
		if err != nil {
			// Column doesn't exist yet. If other errors can be thrown, check type here.
			row.cols[string(cell.ColId)] = &memoryCol{cells: make(map[string]*Cell)}
		} else if _, err = col.getCell(cell.VerId); err == nil {
			return errors.New(fmt.Sprintf("Version \"%v\" already exists\n", cell.VerId))
		}
		cellsToWrite = append(cellsToWrite, cell)
	}

	for _, cell := range cellsToWrite {
		col := row.cols[string(cell.ColId)]
		col.cells[string(cell.VerId.Bytes())] = cell.Clone()
		if col.largestVerId == nil || col.largestVerId.Cmp(cell.VerId) == -1 {
			col.largestVerId = new(big.Int)
			col.largestVerId.Set(cell.VerId)
		}
	}

	return nil
}

func (t *MemoryBigtable) Get(tableName, rowId []byte, cells []*Cell) ([]*Cell, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	table, err := t.getTable(tableName)
	if err != nil {
		return nil, err
	}

	row, err := table.getRow(rowId)
	if err != nil {
		return nil, err
	}

	res := make([]*Cell, 0)
	for _, cell := range cells {
		col, err := row.getCol(cell.ColId)
		if err != nil {
			return nil, err
		}

		if cell.VerId == nil {
			cell.VerId = new(big.Int)
			cell.VerId.Set(col.largestVerId)
		}

		cell, err = col.getCell(cell.VerId)
		if err != nil {
			return nil, err
		}

		res = append(res, cell)
	}

	return res, nil
}

func (t *MemoryBigtable) CreateTable(tableName []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, err := t.getTable(tableName)
	if err == nil {
		return errors.New(fmt.Sprintf("Table \"%v\" already exists\n", tableName))
	}

	t.tables[string(tableName)] = &memoryTable{rows: make(map[string]*memoryRow)}

	return nil
}

func curTimeMillis() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
}
