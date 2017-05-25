package meddb

import (
	"math/big"
	"sync"
	"time"
)

type Bigtable interface {
	Put(tableName []byte, op *PutOp) error
	Get(tableName []byte, op *GetOp) (map[string][]*Cell, error)
	CreateTable(tableName []byte) error
	// TODO(wojtek): Delete
}

type MemoryBigtable struct {
	tables map[string]*memoryTable
	lock   sync.RWMutex // TODO(wojtek): Optimize by having more granular mutexes
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
	}

	// Fill in missing verIds with current time in ms
	op.fillVer(curTimeMillis())

	toAdd := op.cells()
	// Find indexes after which the cell should be added
	idxs := make([]int, len(toAdd))
	for i, cell := range toAdd {
		col, ok := row.cols[string(cell.ColId)]
		if ok {
			idxs[i] = findCell(col, cell.VerId.Int64())
			// If this verId already exists, return error
			if col[idxs[i]].VerId.Cmp(cell.VerId) == 0 {
				return &VerIdAlreadyExists{VerId: cell.VerId}
			}
		}
	}

	// Insert the cells at found indexes
	for i, cell := range toAdd {
		colString := string(cell.ColId)
		col, ok := row.cols[colString]
		if ok {
			row.cols[colString] = append(append(col[:idxs[i]], cell.Clone()), col[idxs[i]:]...)
		} else {
			row.cols[colString] = []*Cell{cell.Clone()}
		}
	}

	return nil
}

func (bt *MemoryBigtable) Get(tableName []byte, op *GetOp) (map[string][]*Cell, error) {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	_, err := bt.getTable(tableName)
	if err != nil {
		return nil, err
	}

	res := make(map[string][]*Cell)
	if op.verId != nil {
		// Strategy: getExact
	} else if op.minVer != nil && op.maxVer != nil {
		// Strategy: getRange
	} else if op.limit != 0 {
		// Strategy: getLimit
	} else {
		// Strategy: getAll
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

func curTimeMillis() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
}
