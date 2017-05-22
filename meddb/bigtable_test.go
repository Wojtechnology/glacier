package meddb

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/test"
)

func assertCellsEqualNoVerId(t *testing.T, a, b *Cell) {
	test.AssertBytesEqual(t, a.ColId, b.ColId)
	test.AssertBytesEqual(t, a.Data, b.Data)
}

func assertCellsEqual(t *testing.T, a, b *Cell) {
	assertCellsEqualNoVerId(t, a, b)
	test.AssertEqual(t, a.VerId, b.VerId)
}

// -----------------------
// Test Memory Table Happy
// -----------------------

func TestMemoryTablePutGet(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	rowId := []byte("asdf")
	cells := make([]*Cell, 2)
	cells[0] = NewCell([]byte("bruh"), nil, []byte("yo"))
	cells[1] = NewCell([]byte("fam"), nil, []byte("hey"))
	err = mem.Put(tableName, rowId, cells)
	assert.Nil(t, err)

	reqCells := make([]*Cell, 2)
	reqCells[0] = NewCell([]byte("bruh"), nil, nil)
	reqCells[1] = NewCell([]byte("fam"), nil, nil)

	newCells, err := mem.Get(tableName, rowId, reqCells)
	test.AssertEqual(t, 2, len(newCells))
	assert.Nil(t, err)

	assertCellsEqualNoVerId(t, cells[0], newCells[0])
	assertCellsEqualNoVerId(t, cells[1], newCells[1])
	assert.NotNil(t, newCells[0].VerId)
	assert.NotNil(t, newCells[1].VerId)
}

func TestMemoryTableLater(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	rowId := []byte("asdf")
	cells := make([]*Cell, 1)
	cells[0] = NewCell([]byte("bruh"), nil, []byte("yo"))
	err = mem.Put(tableName, rowId, cells)
	assert.Nil(t, err)

	// Code is too fast
	time.Sleep(time.Millisecond)

	newerCells := make([]*Cell, 1)
	newerCells[0] = NewCell([]byte("bruh"), nil, []byte("fam"))
	err = mem.Put(tableName, rowId, newerCells)
	assert.Nil(t, err)

	reqCells := make([]*Cell, 1)
	reqCells[0] = NewCell([]byte("bruh"), nil, nil)

	newCells, err := mem.Get(tableName, rowId, reqCells)
	test.AssertEqual(t, 1, len(newCells))
	assert.Nil(t, err)

	assertCellsEqualNoVerId(t, newerCells[0], newCells[0])
	assert.NotNil(t, newCells[0].VerId)
}

func TestMemoryTableWithVerId(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	rowId := []byte("asdf")
	cells := make([]*Cell, 1)
	cells[0] = NewCell([]byte("bruh"), big.NewInt(69), []byte("yo"))
	err = mem.Put(tableName, rowId, cells)
	assert.Nil(t, err)

	reqCells := make([]*Cell, 1)
	reqCells[0] = NewCell([]byte("bruh"), nil, nil)

	newCells, err := mem.Get(tableName, rowId, reqCells)
	test.AssertEqual(t, 1, len(newCells))
	assert.Nil(t, err)

	assertCellsEqual(t, cells[0], newCells[0])
}

// -------------------------
// Test Memory Table Put Sad
// -------------------------

func TestMemoryTableMissingTablePut(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	err = mem.Put([]byte("IAMNOTTABLE"), nil, nil)
	assert.IsType(t, errors.New(""), err)
}

func TestMemoryTableVerIdAlreadyExistsPut(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	cells := make([]*Cell, 1)
	cells[0] = NewCell([]byte("bruh"), big.NewInt(69), []byte("yo"))
	err = mem.Put(tableName, []byte("yo"), cells)
	assert.Nil(t, err)

	err = mem.Put(tableName, []byte("yo"), cells)
	assert.IsType(t, errors.New(""), err)
}

// -------------------------
// Test Memory Table Get Sad
// -------------------------

func TestMemoryTableMissingTableGet(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	_, err = mem.Get([]byte("IAMNOTTABLE"), nil, nil)
	assert.IsType(t, errors.New(""), err)
}

func TestMemoryTableMissingRowGet(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	cells := make([]*Cell, 1)
	cells[0] = NewCell([]byte("bruh"), big.NewInt(69), []byte("yo"))
	_, err = mem.Get(tableName, []byte("yo"), cells)
	assert.IsType(t, errors.New(""), err)
}

func TestMemoryTableMissingColGet(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	cells := make([]*Cell, 1)
	cells[0] = NewCell([]byte("bruh"), big.NewInt(69), []byte("yo"))
	err = mem.Put(tableName, []byte("yo"), cells)
	assert.Nil(t, err)

	cells = make([]*Cell, 1)
	cells[0] = NewCell([]byte("fam"), big.NewInt(69), nil)
	_, err = mem.Get(tableName, []byte("yo"), cells)
	assert.IsType(t, errors.New(""), err)
}

func TestMemoryTableMissingVerIdGet(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	cells := make([]*Cell, 1)
	cells[0] = NewCell([]byte("bruh"), big.NewInt(69), []byte("yo"))
	err = mem.Put(tableName, []byte("yo"), cells)
	assert.Nil(t, err)

	cells = make([]*Cell, 1)
	cells[0] = NewCell([]byte("bruh"), big.NewInt(70), nil)
	_, err = mem.Get(tableName, []byte("yo"), cells)
	assert.IsType(t, errors.New(""), err)
}

// ---------------------------------
// Test Memory Table CreateTable Sad
// ---------------------------------

func TestMemoryTableCreateAlreadyExists(t *testing.T) {
	mem, _ := NewMemoryBigtable()
	tableName := []byte("IAMTABLE")
	err := mem.CreateTable(tableName)
	assert.Nil(t, err)

	err = mem.CreateTable(tableName)
	assert.IsType(t, errors.New(""), err)
}
