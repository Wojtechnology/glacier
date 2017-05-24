package meddb

import (
	"testing"

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

// -----------------------
// Test Memory Table Happy
// -----------------------

// -------------------------
// Test Memory Table Put Sad
// -------------------------

// -------------------------
// Test Memory Table Get Sad
// -------------------------

// ---------------------------------
// Test Memory Table CreateTable Sad
// ---------------------------------
