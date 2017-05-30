package meddb

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"sync"

	r "gopkg.in/gorethink/gorethink.v3"
)

type RethinkBigtable struct {
	session  *r.Session
	lock     sync.RWMutex
	database string
}

type rethinkCell struct {
	ID    []byte `gorethink:"id"`
	RowId []byte `gorethink:"row_id"`
	ColId []byte `gorethink:"col_id"`
	VerId []byte `gorethink:"ver_id"`
	Data  []byte `gorethink:"data"`
}

// -------------------
// RethinkBigtable API
// -------------------

func NewRethinkBigtable(addresses []string, database string) (*RethinkBigtable, error) {
	session, err := r.Connect(r.ConnectOpts{
		Addresses: addresses,
	})
	if err != nil {
		return nil, err
	}
	t := &RethinkBigtable{session: session, database: database}
	return t, nil
}

func (bt *RethinkBigtable) Put(tableName []byte, op *PutOp) error {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	// Fill in missing verIds with current time in ms
	op.fillVer(curTimeMillis())

	rethinkCells := make([]interface{}, len(op.cols))
	for i, cell := range op.cells() {
		rCell, err := newRethinkCell(cell)
		if err != nil {
			return err
		}
		rethinkCells[i] = rCell
	}

	_, err := r.DB(bt.database).Table(string(tableName)).Insert(rethinkCells, r.InsertOpts{
		Conflict: "replace",
	}).RunWrite(bt.session)
	if err != nil {
		if _, ok := err.(r.RQLOpFailedError); ok {
			// TODO(wojtek): Pretty sure this is wrong, but too lazy to figure it out now
			return &TableNotFoundError{TableName: tableName}
		}
		return err
	}

	return nil
}

func (bt *RethinkBigtable) Get(tableName []byte, op *GetOp) (map[string][]*Cell, error) {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	var (
		res       *r.Cursor
		err       error
		tableTerm r.Term = r.DB(bt.database).Table(string(tableName))
	)
	if op.verId != nil {
		// Strategy: getExact
		ids := make([]interface{}, len(op.colIds))
		for i, colId := range op.colIds {
			if ids[i], err = buildRethinkId(op.rowId, colId, op.verId); err != nil {
				return nil, err
			}
		}
		res, err = tableTerm.GetAll(ids...).Run(bt.session)
	} else if op.minVer != nil && op.maxVer != nil {
		// Strategy: getRange
		lo, hi := int64ToBytes(op.minVer.Int64()), int64ToBytes(op.maxVer.Int64())
		res, err = tableTerm.GetAllByIndex(
			"row_id", op.rowId,
		).Filter(func(row r.Term) interface{} {
			return r.Expr(op.colIds).Contains(row.Field("col_id")).And(
				row.Field("ver_id").Ge(lo),
			).And(
				row.Field("ver_id").Le(hi),
			)
		}).OrderBy(r.Desc("ver_id")).Run(bt.session)
	} else if op.limit != 0 {
		// Strategy: getLimit
		res, err = tableTerm.GetAllByIndex(
			"row_id", op.rowId,
		).Filter(func(row r.Term) interface{} {
			return r.Expr(op.colIds).Contains(row.Field("col_id"))
		}).Group("col_id").OrderBy(r.Desc("ver_id")).Limit(op.limit).Run(bt.session)
	} else {
		// Strategy: getAll
		res, err = tableTerm.GetAllByIndex(
			"row_id", op.rowId,
		).Filter(func(row r.Term) interface{} {
			return r.Expr(op.colIds).Contains(row.Field("col_id"))
		}).OrderBy(r.Desc("ver_id")).Run(bt.session)
	}
	defer res.Close()
	if err != nil {
		if _, ok := err.(r.RQLOpFailedError); ok {
			// TODO(wojtek): Pretty sure this is wrong, but too lazy to figure it out now
			return nil, &TableNotFoundError{TableName: tableName}
		}
		return nil, err
	}

	var rows []*rethinkCell
	if op.limit != 0 {
		// Special casing with limit because of group
		var groupMap []map[string]interface{}
		if err = res.All(&groupMap); err != nil {
			return nil, err
		}

		for _, group := range groupMap {
			for _, rowObj := range group["reduction"].([]interface{}) {
				row := rowObj.(map[string]interface{})
				rCell := &rethinkCell{
					ID:    row["id"].([]byte),
					RowId: row["row_id"].([]byte),
					ColId: row["col_id"].([]byte),
					VerId: row["ver_id"].([]byte),
					Data:  row["data"].([]byte),
				}
				rows = append(rows, rCell)
			}
		}
	} else {
		if err := res.All(&rows); err != nil {
			return nil, err
		}
	}

	cells := make(map[string][]*Cell)
	for _, row := range rows {
		cell := NewCellVer(
			row.RowId,
			row.ColId,
			bytesToInt64(row.VerId),
			row.Data,
		)

		if _, ok := cells[string(cell.ColId)]; ok {
			cells[string(cell.ColId)] = append(cells[string(cell.ColId)], cell)
		} else {
			cells[string(cell.ColId)] = []*Cell{cell}
		}
	}

	return cells, nil
}

func (bt *RethinkBigtable) CreateTable(tableName []byte) error {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	_, err := r.DB(bt.database).TableCreate(string(tableName)).Run(bt.session)
	if err != nil {
		if _, ok := err.(r.RQLOpFailedError); ok {
			return &TableAlreadyExists{TableName: tableName}
		}
		return err
	}

	_, err = r.DB(bt.database).Table(string(tableName)).IndexCreate("row_id").RunWrite(bt.session)
	if err != nil {
		return err
	}

	return nil
}

// -------
// Helpers
// -------

func newRethinkCell(cell *Cell) (*rethinkCell, error) {
	if cell.RowId == nil || cell.ColId == nil || cell.VerId == nil || cell.Data == nil {
		return nil, errors.New(fmt.Sprintf(`Cell is missing rowId, colId, verId or data
			rowId %v
			colId %v
			verId %v
			data %v
		`, cell.RowId, cell.ColId, cell.VerId, cell.Data))
	}
	id, err := buildRethinkId(cell.RowId, cell.ColId, cell.VerId)
	if err != nil {
		return nil, err
	}
	return &rethinkCell{
		ID:    id,
		RowId: cell.RowId,
		ColId: cell.ColId,
		VerId: int64ToBytes(cell.VerId.Int64()),
		Data:  cell.Data,
	}, nil
}

func buildRethinkId(rowId, colId []byte, verId *big.Int) ([]byte, error) {
	w := bytes.NewBuffer([]byte{})
	encoder := base64.NewEncoder(base64.StdEncoding, w)
	if _, err := encoder.Write(rowId); err != nil {
		return nil, err
	}
	w.Write([]byte("-"))
	if _, err := encoder.Write(colId); err != nil {
		return nil, err
	}
	w.Write([]byte("-"))
	if _, err := encoder.Write(int64ToBytes(verId.Int64())); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// Converts x to zero-padded byte array. Values are shifted so that smallest int64 becomes
// 0x0000000000000000 and largest becomes 0x1111111111111111 to make range searches including
// positive and negative version ids possible.
func int64ToBytes(x int64) []byte {
	x ^= -9223372036854775808 // 0b100000000000000...
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(x)
		x >>= 8
	}
	return b
}

func bytesToInt64(b []byte) int64 {
	var x int64 = 0
	for i := 0; i < 8; i++ {
		x <<= 8
		x |= int64(b[i])
	}
	x ^= -9223372036854775808 // 0b100000000000000...
	return x
}
