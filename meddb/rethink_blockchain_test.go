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

	txs := rethinkGetBacklog(t, db)

	assert.Equal(t, 1, len(txs))
	assert.Equal(t, tx, txs[0])
}

func TestRethinkGetAssignedTransactions(t *testing.T) {
	db := getRethinkDB(t)
	defer rethinkDeleteBacklog(db)

	pubKey := []byte{69}
	tx := getTestTransaction()
	otherTx := getTestTransaction()
	otherTx.Hash = []byte{22}
	otherTx.AssignedTo = pubKey

	rethinkWriteToBacklog(t, db, []*Transaction{tx, otherTx})

	txs, err := db.GetAssignedTransactions(pubKey)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, otherTx, txs[0])
}

func TestRethinkDeleteTransactions(t *testing.T) {
	db := getRethinkDB(t)
	defer rethinkDeleteBacklog(db)
	tx := getTestTransaction()
	otherTx := getTestTransaction()
	otherTx.Hash = []byte{22}

	rethinkWriteToBacklog(t, db, []*Transaction{tx, otherTx})

	err := db.DeleteTransactions([]*Transaction{tx})
	assert.Nil(t, err)

	txs := rethinkGetBacklog(t, db)

	assert.Equal(t, 1, len(txs))
	assert.Equal(t, otherTx, txs[0])
}

func TestRethinkWriteBlock(t *testing.T) {
	db := getRethinkDB(t)
	defer rethinkDeleteBlocks(db)
	b := getTestBlock()

	err := db.WriteBlock(b)
	assert.Nil(t, err)

	bs := rethinkGetBlocks(t, db)

	assert.Equal(t, 1, len(bs))
	assert.Equal(t, b, bs[0])
}

func TestRethinkWriteVote(t *testing.T) {
	db := getRethinkDB(t)
	defer rethinkDeleteVotes(db)
	v := getTestVote()

	err := db.WriteVote(v)
	assert.Nil(t, err)

	vs := rethinkGetVotes(t, db)

	assert.Equal(t, 1, len(vs))
	assert.Equal(t, v, vs[0])
}

// ------------
// Test Helpers
// ------------

func TestRethinkTransactionMapper(t *testing.T) {
	tx := getTestTransaction()
	assert.Equal(t, tx, fromRethinkTransaction(newRethinkTransaction(tx)))
}

func TestRethinkBlockMapper(t *testing.T) {
	b := getTestBlock()
	assert.Equal(t, b, fromRethinkBlock(newRethinkBlock(b)))
}

func TestRethinkVoteMapper(t *testing.T) {
	v := getTestVote()
	assert.Equal(t, v, fromRethinkVote(newRethinkVote(v)))
}

// -------
// Helpers
// -------

func getRethinkDB(t *testing.T) *RethinkBlockchainDB {
	db, err := NewRethinkBlockchainDB([]string{"127.0.0.1"}, rethinkBlockchainDB)
	assert.Nil(t, err)
	return db
}

func rethinkWriteToBacklog(t *testing.T, db *RethinkBlockchainDB, txs []*Transaction) {
	for _, tx := range txs {
		_, err := db.backlogTable().Insert(newRethinkTransaction(tx)).RunWrite(db.session)
		assert.Nil(t, err)
	}
}

func rethinkGetBacklog(t *testing.T, db *RethinkBlockchainDB) []*Transaction {
	cur, err := db.backlogTable().Run(db.session)
	assert.Nil(t, err)

	var res []*rethinkTransaction
	err = cur.All(&res)
	assert.Nil(t, err)

	txs := make([]*Transaction, len(res))
	for i, tx := range res {
		txs[i] = fromRethinkTransaction(tx)
	}
	return txs
}

func rethinkGetBlocks(t *testing.T, db *RethinkBlockchainDB) []*Block {
	cur, err := db.blockTable().Run(db.session)
	assert.Nil(t, err)

	var res []*rethinkBlock
	err = cur.All(&res)
	assert.Nil(t, err)

	bs := make([]*Block, len(res))
	for i, b := range res {
		bs[i] = fromRethinkBlock(b)
	}
	return bs
}

func rethinkGetVotes(t *testing.T, db *RethinkBlockchainDB) []*Vote {
	cur, err := db.voteTable().Run(db.session)
	assert.Nil(t, err)

	var res []*rethinkVote
	err = cur.All(&res)
	assert.Nil(t, err)

	vs := make([]*Vote, len(res))
	for i, v := range res {
		vs[i] = fromRethinkVote(v)
	}
	return vs
}

func rethinkDeleteBacklog(db *RethinkBlockchainDB) {
	db.backlogTable().Delete().RunWrite(db.session)
}

func rethinkDeleteBlocks(db *RethinkBlockchainDB) {
	db.blockTable().Delete().RunWrite(db.session)
}

func rethinkDeleteVotes(db *RethinkBlockchainDB) {
	db.voteTable().Delete().RunWrite(db.session)
}
