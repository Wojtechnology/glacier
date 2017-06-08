package meddb

import (
	"math/big"
	"sync"

	r "gopkg.in/gorethink/gorethink.v3"
)

const (
	rethinkBacklogName = "backlog"
	rethinkBlockName   = "block"
)

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
	Data         []byte              `gorethink:"data"`
}

type rethinkBlock struct {
	Hash         []byte   `gorethink:"id"`
	Transactions [][]byte `gorethink:"transactions"`
	CreatedAt    []byte   `gorethink:"created_at"`
	Creator      []byte   `gorethink:"creator"`
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

	_, err := r.DBCreate(db.database).RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = r.DB(db.database).TableCreate(rethinkBacklogName).RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = r.DB(db.database).TableCreate(rethinkBlockName).RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = r.DB(db.database).Table(rethinkBacklogName).IndexCreate(
		"assigned_to",
	).RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = r.DB(db.database).Table(rethinkBacklogName).IndexWait().Run(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) WriteTransaction(tx *Transaction) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	rethinkTx := newRethinkTransaction(tx)
	_, err := db.backlogTable().Insert(rethinkTx, r.InsertOpts{
		Conflict: "replace",
	}).RunWrite(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) GetAssignedTransactions(pubKey []byte) ([]*Transaction, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	res, err := db.backlogTable().GetAllByIndex("assigned_to", pubKey).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkTransaction
	if err := res.All(&rows); err != nil {
		return nil, err
	}

	txs := make([]*Transaction, len(rows))
	for i, row := range rows {
		txs[i] = fromRethinkTransaction(row)
	}
	return txs, nil
}

func (db *RethinkBlockchainDB) DeleteTransactions(txs []*Transaction) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	ids := make([]interface{}, len(txs))
	for i, tx := range txs {
		ids[i] = tx.Hash
	}

	_, err := db.backlogTable().GetAll(ids...).Delete().RunWrite(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) WriteBlock(b *Block) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	rethinkB := newRethinkBlock(b)
	_, err := db.blockTable().Insert(rethinkB, r.InsertOpts{
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

func (db *RethinkBlockchainDB) backlogTable() r.Term {
	return r.DB(db.database).Table(rethinkBacklogName)
}

func (db *RethinkBlockchainDB) blockTable() r.Term {
	return r.DB(db.database).Table(rethinkBlockName)
}

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
		Data:         tx.Data,
	}
}

func fromRethinkTransaction(tx *rethinkTransaction) *Transaction {
	var lastAssigned *big.Int = nil
	if tx.LastAssigned != nil {
		lastAssigned = big.NewInt(bytesToInt64(tx.LastAssigned))
	}
	var cellAddress *CellAddress = nil
	if tx.CellAddress != nil {
		var verId *big.Int = nil
		if tx.CellAddress.VerId != nil {
			verId = big.NewInt(bytesToInt64(tx.CellAddress.VerId))
		}

		cellAddress = &CellAddress{
			TableName: tx.CellAddress.TableName,
			RowId:     tx.CellAddress.RowId,
			ColId:     tx.CellAddress.ColId,
			VerId:     verId,
		}
	}

	return &Transaction{
		Hash:         tx.Hash,
		AssignedTo:   tx.AssignedTo,
		LastAssigned: lastAssigned,
		CellAddress:  cellAddress,
		Data:         tx.Data,
	}

}

func newRethinkBlock(b *Block) *rethinkBlock {
	var createdAt []byte = nil
	if b.CreatedAt != nil {
		createdAt = int64ToBytes(b.CreatedAt.Int64())
	}

	return &rethinkBlock{
		Hash:         b.Hash,
		Transactions: b.Transactions,
		CreatedAt:    createdAt,
		Creator:      b.Creator,
	}
}

func fromRethinkBlock(b *rethinkBlock) *Block {
	var createdAt *big.Int = nil
	if b.CreatedAt != nil {
		createdAt = big.NewInt(bytesToInt64(b.CreatedAt))
	}

	return &Block{
		Hash:         b.Hash,
		Transactions: b.Transactions,
		CreatedAt:    createdAt,
		Creator:      b.Creator,
	}
}
