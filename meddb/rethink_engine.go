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
	ID    string `gorethink:"id"`
	RowId string `gorethink:"row_id"`
	ColId string `gorethink:"col_id"`
	VerId string `gorethink:"ver_id"`
	Data  string `gorethink:"data"`
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
		fmt.Printf("%s, len(%d)\n", rCell.ID, len(rCell.ID))
	}

	_, err := r.DB(bt.database).Table(string(tableName)).Insert(rethinkCells, r.InsertOpts{
		Conflict: "replace",
	}).RunWrite(bt.session)
	if err != nil {
		return err
	}

	return nil
}

func (bt *RethinkBigtable) Get(tableName []byte, op *GetOp) (map[string][]*Cell, error) {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	// var res *r.Cursor
	if op.verId != nil {
		// Strategy: getExact
	} else if op.minVer != nil && op.maxVer != nil {
		// Strategy: getRange
	} else {
		// Strategy: getLimit or getAll
	}

	return nil, nil
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
		RowId: string(cell.RowId),
		ColId: string(cell.ColId),
		VerId: string(int64ToBytes(cell.VerId.Int64())),
		Data:  string(cell.Data),
	}, nil
}

func buildRethinkId(rowId, colId []byte, verId *big.Int) (string, error) {
	w := bytes.NewBufferString("")
	encoder := base64.NewEncoder(base64.StdEncoding, w)
	if _, err := encoder.Write(rowId); err != nil {
		return "", err
	}
	w.Write([]byte("-"))
	if _, err := encoder.Write(colId); err != nil {
		return "", err
	}
	w.Write([]byte("-"))
	if _, err := encoder.Write(int64ToBytes(verId.Int64())); err != nil {
		return "", err
	}
	if err := encoder.Close(); err != nil {
		return "", err
	}
	return w.String(), nil
}

func int64ToBytes(x int64) []byte {
	// TODO(wojtek): make this a rethink specific method that shifts int64 to uint64 scale so
	// that range searches work
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(x)
		x >>= 8
	}
	return b
}
