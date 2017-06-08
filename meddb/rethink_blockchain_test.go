package meddb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	r "gopkg.in/gorethink/gorethink.v3"
)

const rethinkBlockchainDB = "test_blockchain"

func init() {
	session, err := r.Connect(r.ConnectOpts{Addresses: []string{"127.0.0.1"}})
	if err != nil {
		panic(err)
	}
	r.DBDrop(rethinkBlockchainDB).Run(session)
	db, err := NewRethinkBlockchainDB([]string{"127.0.0.1"}, rethinkBlockchainDB)
	if err != nil {
		panic(err)
	}
	if err := db.SetupTables(); err != nil {
		panic(err)
	}
}

// ------------------------
// Test RethinkBlockchainDB
// ------------------------

func TestRethinkWriteTransaction(t *testing.T) {
	db := getRethinkDB(t)
	defer rethinkDeleteBacklog(db)
	tx := getTestTransaction()

	err := db.WriteTransaction(tx)
	assert.Nil(t, err)

	cur, err := r.DB(rethinkBlockchainDB).Table(rethinkBacklogName).Run(db.session)
	assert.Nil(t, err)

	var res []*rethinkTransaction
	err = cur.All(&res)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(res))
	assert.Equal(t, tx, fromRethinkTransaction(res[0]))
}

func TestRethinkGetAssignedTransactions(t *testing.T) {
	db := getRethinkDB(t)
	defer rethinkDeleteBacklog(db)

	pubKey := []byte{69}
	tx := getTestTransaction()
	otherTx := getTestTransaction()
	otherTx.Hash = []byte{22}
	otherTx.AssignedTo = pubKey

	_, err := r.DB(rethinkBlockchainDB).Table(rethinkBacklogName).Insert(
		newRethinkTransaction(tx),
	).RunWrite(db.session)
	assert.Nil(t, err)
	_, err = r.DB(rethinkBlockchainDB).Table(rethinkBacklogName).Insert(
		newRethinkTransaction(otherTx),
	).RunWrite(db.session)
	assert.Nil(t, err)

	txs, err := db.GetAssignedTransactions(pubKey)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, otherTx, txs[0])
}

// ------------
// Test Helpers
// ------------

func TestRethinkTransactionMapper(t *testing.T) {
	tx := getTestTransaction()
	assert.Equal(t, tx, fromRethinkTransaction(newRethinkTransaction(tx)))
}

// -------
// Helpers
// -------

func getRethinkDB(t *testing.T) *RethinkBlockchainDB {
	db, err := NewRethinkBlockchainDB([]string{"127.0.0.1"}, rethinkBlockchainDB)
	assert.Nil(t, err)
	return db
}

func rethinkDeleteBacklog(db *RethinkBlockchainDB) {
	r.DB(rethinkBlockchainDB).Table(rethinkBacklogName).Delete().Run(db.session)
}
