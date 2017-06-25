package meddb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/test"
)

func assertCellsEqualNoVerId(t *testing.T, a, b *Cell) {
	test.AssertBytesEqual(t, a.Data, b.Data)
}

func assertCellsEqual(t *testing.T, a, b *Cell) {
	assertCellsEqualNoVerId(t, a, b)
	test.AssertEqual(t, a.VerId, b.VerId)
}

// -------------------
// Test Put/Get Memory
// -------------------

func TestMemoryPutGet(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testPutGet(t, bt, memoryCreateTable(t, bt))
}

func TestMemoryPutGetEmpty(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testPutGetEmpty(t, bt, memoryCreateTable(t, bt))
}

func TestMemoryPutGetVer(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testPutGetVer(t, bt, memoryCreateTable(t, bt))
}

func TestMemoryPutOverwrite(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testPutOverwrite(t, bt, memoryCreateTable(t, bt))
}

func TestMemoryGetExact(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testGetExact(t, bt, memoryCreateTable(t, bt))
}

func TestMemoryGetLimit(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testGetLimit(t, bt, memoryCreateTable(t, bt))
}

func TestMemoryGetRange(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testGetRange(t, bt, memoryCreateTable(t, bt))
}

func TestMemoryPutTableNotFound(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testPutTableNotFound(t, bt)
}

func TestMemoryGetTableNotFound(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testGetTableNotFound(t, bt)
}

func TestMemoryCreateTableAlreadyExists(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testCreateTableAlreadyExists(t, bt)
}

// ------------
// Test Helpers
// ------------

func TestFindCell(t *testing.T) {
	cells := make([]*Cell, 3)
	cells[0] = NewCellVer(9, nil)
	cells[1] = NewCellVer(7, nil)
	cells[2] = NewCellVer(5, nil)

	oddCases := [][]int{
		{10, 0}, {9, 0},
		{8, 1}, {7, 1},
		{6, 2}, {5, 2},
		{4, 3},
	}

	// Test with an odd list
	for _, testCase := range oddCases {
		test.AssertEqual(t, testCase[1], findCell(cells, int64(testCase[0])))
	}

	cells = append(cells, NewCellVer(3, nil))
	evenCases := append(oddCases, []int{3, 3}, []int{2, 4})

	// Test with an even list
	for _, testCase := range evenCases {
		test.AssertEqual(t, testCase[1], findCell(cells, int64(testCase[0])))
	}
}

func TestFindCellEmpty(t *testing.T) {
	test.AssertEqual(t, -1, findCell(nil, 0))
	test.AssertEqual(t, -1, findCell(make([]*Cell, 0), 0))
}

func putAndCheck(t *testing.T, bt Bigtable, tableName, rowId, colId, data []byte) {
	putOp := NewPutOp(rowId)
	putOp.AddCol(colId, data)

	err := bt.Put(tableName, putOp)
	assert.Nil(t, err)
}

func putAndCheckVer(t *testing.T, bt Bigtable, tableName, rowId, colId []byte, verId int64,
	data []byte) {
	putOp := NewPutOp(rowId)
	putOp.AddColVer(colId, verId, data)

	err := bt.Put(tableName, putOp)
	assert.Nil(t, err)
}

func putVerCells(t *testing.T, bt Bigtable, tableName, rowId, colId []byte, verIds []int64,
	data []byte) {
	for _, verId := range verIds {
		putAndCheckVer(t, bt, tableName, rowId, colId, verId, data)
	}
}

func memoryCreateTable(t *testing.T, bt *MemoryBigtable) []byte {
	tableName := []byte("HELLO")
	err := bt.CreateTable(tableName)
	assert.Nil(t, err)
	return tableName
}
