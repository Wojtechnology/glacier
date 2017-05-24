package meddb

import (
	"sync"
	"time"
)

type Bigtable interface {
	Put(tableName []byte, op *PutOp) error
	Get(tableName []byte, op *GetOp) (map[string]*Cell, error)
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
		return nil
	}

	_, err = table.getRow(op.rowId)
	if err != nil {
		_ = &memoryRow{cols: make(map[string][]*Cell)}
	}

	// Fill in missing versions, casting time to unsigned int
	op.fillVer(curTimeMillis())

	return nil
}

func (bt *MemoryBigtable) Get(tableName []byte, op *GetOp) (map[string]*Cell, error) {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	return nil, nil
}

func (bt *MemoryBigtable) CreateTable(tableName []byte) error {
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

func curTimeMillis() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
}
