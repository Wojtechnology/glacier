package meddb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/test"
)

func assertCellsEqualNoVerId(t *testing.T, a, b *Cell) {
	test.AssertBytesEqual(t, a.RowId, b.RowId)
	test.AssertBytesEqual(t, a.ColId, b.ColId)
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

// -------------------
// Test Put/Get Common
// -------------------

func testPutGet(t *testing.T, bt Bigtable, tableName []byte) {
	rowId := []byte("AYY LMAO")
	colId := []byte("YO FAM")
	data := []byte("OH SHIT WADDUP")

	putAndCheck(t, bt, tableName, rowId, colId, data)

	getOp := NewGetOp(rowId, [][]byte{colId})

	res, err := bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res[string(colId)]))
	assertCellsEqualNoVerId(t, NewCell(rowId, colId, data), res[string(colId)][0])
	assert.NotNil(t, res[string(colId)][0].VerId)
}

func testPutGetEmpty(t *testing.T, bt Bigtable, tableName []byte) {
	rowId := []byte("AYY LMAO")
	colId := []byte("YO FAM")

	getOp := NewGetOp(rowId, [][]byte{colId})

	res, err := bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res))
}

func testPutGetVer(t *testing.T, bt Bigtable, tableName []byte) {
	rowId := []byte("AYY LMAO")
	colId := []byte("YO FAM")
	data := []byte("OH SHIT WADDUP")

	putVerCells(t, bt, tableName, rowId, colId, []int64{3, 1, 7, 5}, data)

	getOp := NewGetOp(rowId, [][]byte{colId})

	res, err := bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(res[string(colId)]))
	assertCellsEqual(t, NewCellVer(rowId, colId, 7, data), res[string(colId)][0])
	assertCellsEqual(t, NewCellVer(rowId, colId, 5, data), res[string(colId)][1])
	assertCellsEqual(t, NewCellVer(rowId, colId, 3, data), res[string(colId)][2])
	assertCellsEqual(t, NewCellVer(rowId, colId, 1, data), res[string(colId)][3])
}

func testPutOverwrite(t *testing.T, bt Bigtable, tableName []byte) {
	rowId := []byte("AYY LMAO")
	colId := []byte("YO FAM")
	verId := int64(69)
	data := []byte("OH SHIT WADDUP")

	putAndCheckVer(t, bt, tableName, rowId, colId, verId, data)

	getOp := NewGetOp(rowId, [][]byte{colId})

	res, err := bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res[string(colId)]))
	assertCellsEqual(t, NewCellVer(rowId, colId, verId, data), res[string(colId)][0])
	assert.NotNil(t, res[string(colId)][0].VerId)

	data = []byte("YOO I CHANGED")

	putAndCheckVer(t, bt, tableName, rowId, colId, verId, data)

	res, err = bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res[string(colId)]))
	assertCellsEqual(t, NewCellVer(rowId, colId, verId, data), res[string(colId)][0])
	assert.NotNil(t, res[string(colId)][0].VerId)
}

func testGetExact(t *testing.T, bt Bigtable, tableName []byte) {
	rowId := []byte("AYY LMAO")
	colId := []byte("YO FAM")
	data := []byte("OH SHIT WADDUP")

	putVerCells(t, bt, tableName, rowId, colId, []int64{3, 1, 7, 5}, data)

	getOp := NewGetOpVer(rowId, [][]byte{colId}, 3)

	res, err := bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res[string(colId)]))
	assertCellsEqual(t, NewCellVer(rowId, colId, 3, data), res[string(colId)][0])

	getOp = NewGetOpVer(rowId, [][]byte{colId}, 4)

	res, err = bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res[string(colId)]))
}

func testGetLimit(t *testing.T, bt Bigtable, tableName []byte) {
	rowId := []byte("AYY LMAO")
	colId := []byte("YO FAM")
	data := []byte("OH SHIT WADDUP")

	putVerCells(t, bt, tableName, rowId, colId, []int64{3, 1, 7, 5}, data)

	getOp := NewGetOpLimit(rowId, [][]byte{colId}, 2)

	res, err := bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res[string(colId)]))
	assertCellsEqual(t, NewCellVer(rowId, colId, 7, data), res[string(colId)][0])
	assertCellsEqual(t, NewCellVer(rowId, colId, 5, data), res[string(colId)][1])
}

func testGetRange(t *testing.T, bt Bigtable, tableName []byte) {
	rowId := []byte("AYY LMAO")
	colId := []byte("YO FAM")
	data := []byte("OH SHIT WADDUP")

	putVerCells(t, bt, tableName, rowId, colId, []int64{3, 1, 7, 5}, data)

	getOp := NewGetOpRange(rowId, [][]byte{colId}, 3, 5)

	res, err := bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res[string(colId)]))
	assertCellsEqual(t, NewCellVer(rowId, colId, 5, data), res[string(colId)][0])
	assertCellsEqual(t, NewCellVer(rowId, colId, 3, data), res[string(colId)][1])

	getOp = NewGetOpRange(rowId, [][]byte{colId}, 2, 6)

	res, err = bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res[string(colId)]))
	assertCellsEqual(t, NewCellVer(rowId, colId, 5, data), res[string(colId)][0])
	assertCellsEqual(t, NewCellVer(rowId, colId, 3, data), res[string(colId)][1])

	getOp = NewGetOpRange(rowId, [][]byte{colId}, -1, 10)

	res, err = bt.Get(tableName, getOp)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(res[string(colId)]))
	assertCellsEqual(t, NewCellVer(rowId, colId, 7, data), res[string(colId)][0])
	assertCellsEqual(t, NewCellVer(rowId, colId, 5, data), res[string(colId)][1])
	assertCellsEqual(t, NewCellVer(rowId, colId, 3, data), res[string(colId)][2])
	assertCellsEqual(t, NewCellVer(rowId, colId, 1, data), res[string(colId)][3])
}

func testPutTableNotFound(t *testing.T, bt Bigtable) {
	err := bt.Put([]byte("IAMNOTINTHEDB"), new(PutOp))
	assert.IsType(t, &TableNotFoundError{}, err)
}

func testGetTableNotFound(t *testing.T, bt Bigtable) {
	_, err := bt.Get([]byte("IAMNOTINTHEDB"), new(GetOp))
	assert.IsType(t, &TableNotFoundError{}, err)
}

func testCreateTableAlreadyExists(t *testing.T, bt Bigtable) {
	tableName := []byte("FAMDONTPUTMEIN")

	err := bt.CreateTable(tableName)
	assert.Nil(t, err)

	err = bt.CreateTable(tableName)
	assert.IsType(t, &TableAlreadyExists{}, err)
}

// ------------
// Test Helpers
// ------------

func TestFindCell(t *testing.T) {
	cells := make([]*Cell, 3)
	cells[0] = NewCellVer(nil, nil, 9, nil)
	cells[1] = NewCellVer(nil, nil, 7, nil)
	cells[2] = NewCellVer(nil, nil, 5, nil)

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

	cells = append(cells, NewCellVer(nil, nil, 3, nil))
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
