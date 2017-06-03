package meddb

import (
	"sync"

	r "gopkg.in/gorethink/gorethink.v3"
)

const rethinkBacklogName = "backlog"

type RethinkBlockchainDB struct {
	session  *r.Session
	lock     sync.RWMutex
	database string
}

type rethinkCellAddress struct {
	TableName []byte `gorethink:"table_name"`
	RowId     []byte `gorethink:"row_id"`
	ColId     []byte `gorethink:"col_id"`
	VerId     []byte `gorethink:"ver_id"`
}

type rethinkTransaction struct {
	Hash         []byte              `gorethink:"id"`
	AssignedTo   []byte              `gorethink:"assigned_to"`
	LastAssigned []byte              `gorethink:"last_assigned"`
	CellAddress  *rethinkCellAddress `gorethink:"cell_address"`
}

// ----------------------
// MemoryBlockchainDB API
// ----------------------

func NewRethinkBlockchainDB(addresses []string, database string) (*RethinkBlockchainDB, error) {
	session, err := r.Connect(r.ConnectOpts{
		Addresses: addresses,
	})
	if err != nil {
		return nil, err
	}
	t := &RethinkBlockchainDB{session: session, database: database}
	return t, nil
}

func (db *RethinkBlockchainDB) SetupTables() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if _, err := r.DBCreate(db.database).Run(db.session); err != nil {
		return err
	}
	if _, err := r.DB(db.database).TableCreate(rethinkBacklogName).Run(db.session); err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) WriteTransaction(tx *Transaction) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	rethinkTx := newRethinkTransaction(tx)
	_, err := r.DB(db.database).Table(rethinkBacklogName).Insert(rethinkTx, r.InsertOpts{
		Conflict: "replace",
	}).RunWrite(db.session)
	if err != nil {
		return err
	}

	return nil
}

// -------
// Helpers
// -------

func newRethinkTransaction(tx *Transaction) *rethinkTransaction {
	var lastAssigned []byte = nil
	if tx.LastAssigned != nil {
		lastAssigned = int64ToBytes(tx.LastAssigned.Int64())
	}
	var cellAddress *rethinkCellAddress = nil
	if tx.CellAddress != nil {
		var verId []byte = nil
		if tx.CellAddress.VerId != nil {
			verId = int64ToBytes(tx.CellAddress.VerId.Int64())
		}

		cellAddress = &rethinkCellAddress{
			TableName: tx.CellAddress.TableName,
			RowId:     tx.CellAddress.RowId,
			ColId:     tx.CellAddress.ColId,
			VerId:     verId,
		}
	}

	return &rethinkTransaction{
		Hash:         tx.Hash,
		AssignedTo:   tx.AssignedTo,
		LastAssigned: lastAssigned,
		CellAddress:  cellAddress,
	}
}
