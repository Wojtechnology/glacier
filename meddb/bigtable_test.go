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

// --------
// Test Put
// --------

func TestMemoryPutGet(t *testing.T) {
	bt, err := NewMemoryBigtable()
	assert.Nil(t, err)
	testPutGet(t, bt)
}

// ---------------
// Test Put Common
// ---------------

func testPutGet(t *testing.T, bt Bigtable) {
	tableName := []byte("I AM TABLE")
	rowId := []byte("AYY LMAO")
	colId := []byte("YO FAM")
	data := []byte("OH SHIT WADDUP")

	op := NewPutOp(rowId)
	op.AddCol(colId, data)

	err := bt.CreateTable(tableName)
	assert.Nil(t, err)

	err = bt.Put(tableName, op)
	assert.Nil(t, err)

	// Test get
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
