package meddb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
