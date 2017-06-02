package meddb

import (
	"math/big"
	"sync"
)

type MemoryBigtable struct {
	tables map[string]*memoryTable
	lock   sync.RWMutex
}

type memoryTable struct {
	rows map[string]*memoryRow
}

type memoryRow struct {
	cols map[string][]*Cell
}

// ------------------
// MemoryBigtable API
// ------------------

func NewMemoryBigtable() (*MemoryBigtable, error) {
	return &MemoryBigtable{tables: make(map[string]*memoryTable)}, nil
}

func (bt *MemoryBigtable) Put(tableName []byte, op *PutOp) error {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	table, err := bt.getTable(tableName)
	if err != nil {
		return err
	}

	row, err := table.getRow(op.rowId)
	if err != nil {
		row = &memoryRow{cols: make(map[string][]*Cell)}
		table.rows[string(op.rowId)] = row
	}

	// Fill in missing verIds with current time in ms
	op.fillVer(curTimeMillis())

	for _, cell := range op.cells() {
		colString := string(cell.ColId)
		col, ok := row.cols[colString]
		if ok {
			idx := findCell(col, cell.VerId.Int64())
			// If this verId already exists, return error
			if idx < len(col) && col[idx].VerId.Cmp(cell.VerId) == 0 {
				col[idx] = cell.Clone()
			} else {
				// Add space for one more element
				row.cols[colString] = append(col, nil)
				col = row.cols[colString]
				// Shift tail elements by 1
				for i := len(col) - 1; i > idx; i-- {
					col[i] = col[i-1]
				}
				// Insert actual cell
				col[idx] = cell.Clone()
			}
		} else {
			row.cols[colString] = []*Cell{cell.Clone()}
		}
	}

	return nil
}

func (bt *MemoryBigtable) Get(tableName []byte, op *GetOp) (map[string][]*Cell, error) {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	table, err := bt.getTable(tableName)
	if err != nil {
		return nil, err
	}

	res := make(map[string][]*Cell)
	row, err := table.getRow(op.rowId)
	if err != nil {
		// Row doesn't exist, return empty map
		return res, nil
	}

	for _, colId := range op.colIds {
		colString := string(colId)
		col, ok := row.cols[colString]
		if ok {
			if op.verId != nil {
				// Strategy: getExact
				idx := findCell(col, op.verId.Int64())
				if idx < len(col) && col[idx].VerId.Cmp(op.verId) == 0 {
					res[colString] = []*Cell{col[idx].Clone()}
				}
			} else if op.minVer != nil && op.maxVer != nil {
				// Strategy: getRange
				// Since this is sorted by decreasing, the minimum is actually a higher index
				hi := findCell(col, op.minVer.Int64())
				if hi < len(col) && col[hi].VerId.Cmp(op.minVer) == 0 {
					// This will allow us to pick up a value on the boundary
					hi++
				}
				lo := findCell(col, op.maxVer.Int64())
				for i := lo; i < hi; i++ {
					if res[colString] == nil {
						res[colString] = []*Cell{col[i].Clone()}
					} else {
						res[colString] = append(res[colString], col[i].Clone())
					}
				}
			} else {
				// Strategy: getLimit or getAll
				res[colString] = make([]*Cell, 0)
				for i, cell := range col {
					if op.limit > 0 && uint32(i) >= op.limit {
						break
					}
					res[colString] = append(res[colString], cell.Clone())
				}
			}
		}
	}

	return res, nil
}

func (bt *MemoryBigtable) CreateTable(tableName []byte) error {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	// Check that table doesn't already exist
	if _, ok := bt.tables[string(tableName)]; ok {
		return &TableAlreadyExists{TableName: tableName}
	}

	bt.tables[string(tableName)] = &memoryTable{rows: make(map[string]*memoryRow)}

	return nil
}

// -------
// Helpers
// -------

func (bt *MemoryBigtable) getTable(tableName []byte) (*memoryTable, error) {
	table, ok := bt.tables[string(tableName)]
	if !ok {
		return nil, &TableNotFoundError{TableName: tableName}
	}
	return table, nil
}

func (t *memoryTable) getRow(rowId []byte) (*memoryRow, error) {
	row, ok := t.rows[string(rowId)]
	if !ok {
		return nil, &RowNotFoundError{RowId: rowId}
	}
	return row, nil
}

// Does binary search on cells (assuming they are sorted by decreasing verId).
// Returns the index of the cell if it exists, otherwise, returns the index of the cell with
// the largest verId that is smaller than the target.
func findCell(cells []*Cell, target int64) int {
	if cells == nil || len(cells) == 0 {
		return -1
	}
	l, r := 0, len(cells)-1
	bigTarget := big.NewInt(target)

	for l < r {
		m := (l + r) / 2
		if cells[m].VerId.Cmp(bigTarget) == 0 {
			return m
		} else if cells[m].VerId.Cmp(bigTarget) < 0 {
			r = m - 1
		} else {
			l = m + 1
		}
	}

	if cells[l].VerId.Cmp(bigTarget) <= 0 {
		return l
	} else {
		return l + 1
	}
}
