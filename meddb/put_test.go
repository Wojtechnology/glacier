package meddb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddCol(t *testing.T) {
	rowId := []byte("LOLLL")
	colId := []byte("AYY LMAO")
	op := NewPutOp(rowId)

	err := op.AddCol(colId, nil)
	assert.Nil(t, err)
	assertCellsEqual(t, NewCell(nil), op.cols[string(colId)])
}

func TestAddColVer(t *testing.T) {
	rowId := []byte("LOLLL")
	colId := []byte("AYY LMAO")
	var verId int64 = 123
	op := NewPutOp(rowId)

	err := op.AddColVer(colId, verId, nil)
	assert.Nil(t, err)
	assertCellsEqual(t, NewCellVer(verId, nil), op.cols[string(colId)])
}

func TestAddColAlreadyExists(t *testing.T) {
	rowId := []byte("LOLLL")
	colId := []byte("AYY LMAO")
	op := NewPutOp(rowId)

	err := op.AddCol(colId, nil)
	assert.Nil(t, err)

	err = op.AddCol(colId, nil)
	assert.IsType(t, &ColIdAlreadyExists{}, err)
}

func TestAddColVerAlreadyExists(t *testing.T) {
	rowId := []byte("LOLLL")
	colId := []byte("AYY LMAO")
	var verId int64 = 123
	op := NewPutOp(rowId)

	err := op.AddColVer(colId, verId, nil)
	assert.Nil(t, err)

	verId = 234
	err = op.AddColVer(colId, verId, nil)
	assert.IsType(t, &ColIdAlreadyExists{}, err)
}

func TestFillVer(t *testing.T) {
	rowId := []byte("LOLLL")
	colId := []byte("AYY LMAO")
	op := NewPutOp(rowId)
	op.AddCol(colId, nil)

	var verId int64 = 123
	op.fillVer(verId)
	assertCellsEqual(t, NewCellVer(verId, nil), op.cols[string(colId)])
}
