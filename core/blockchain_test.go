package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/common"
	"github.com/wojtechnology/glacier/meddb"
)

func assertRecent(t *testing.T, x int64) {
	assert.True(t, x >= common.Now()-1000)
	assert.True(t, x <= common.Now())
}

func TestAddTransaction(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	other := &Node{PubKey: []byte{69}}
	tx := &Transaction{
		TableName: []byte{123},
		RowId:     []byte{124},
		Cols: map[string]*Cell{
			string([]byte{125}): &Cell{
				VerId: big.NewInt(126),
				Data:  []byte{127},
			},
		},
	}

	bc := NewBlockchain(db, nil, []*Node{other})

	err = bc.AddTransaction(tx)
	assert.Nil(t, err)
	assert.Equal(t, other.PubKey, tx.AssignedTo)
	assertRecent(t, tx.AssignedAt.Int64())

	txs, err := db.GetAssignedTransactions(other.PubKey)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, tx, fromDBTransaction(txs[0]))
}

func TestGetMyTransactions(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	me := &Node{PubKey: []byte{69}}
	other := &Node{PubKey: []byte{70}}
	tx := &Transaction{
		AssignedTo: me.PubKey,
		TableName:  []byte{1},
	}
	otherTx := &Transaction{
		AssignedTo: other.PubKey,
		TableName:  []byte{2},
	}
	err = db.WriteTransaction(tx.toDBTransaction())
	assert.Nil(t, err)
	err = db.WriteTransaction(otherTx.toDBTransaction())
	assert.Nil(t, err)

	bc := NewBlockchain(db, me, []*Node{other})

	txs, err := bc.GetMyTransactions()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, tx, txs[0])
}

func TestGetStaleTransactions(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	me := &Node{PubKey: []byte{69}}
	other := &Node{PubKey: []byte{70}}
	tx := &Transaction{
		AssignedTo: me.PubKey,
		AssignedAt: big.NewInt(common.Now() - 35000),
		TableName:  []byte{1},
	}
	otherTx := &Transaction{
		AssignedTo: other.PubKey,
		AssignedAt: big.NewInt(common.Now() - 25000),
		TableName:  []byte{2},
	}
	otherTx2 := &Transaction{
		AssignedTo: other.PubKey,
		AssignedAt: big.NewInt(common.Now() - 35000),
		TableName:  []byte{3},
	}
	err = db.WriteTransaction(tx.toDBTransaction())
	assert.Nil(t, err)
	err = db.WriteTransaction(otherTx.toDBTransaction())
	assert.Nil(t, err)
	err = db.WriteTransaction(otherTx2.toDBTransaction())
	assert.Nil(t, err)

	bc := NewBlockchain(db, me, []*Node{other})

	txs, err := bc.GetStaleTransactions(30000)
	assert.Nil(t, err)
	expected := []*Transaction{tx, otherTx2}
	assert.Equal(t, 2, len(txs))
	assert.Subset(t, expected, txs)
	assert.Subset(t, txs, expected)
}

func TestDeleteTransactions(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	me := &Node{PubKey: []byte{69}}
	tx := &Transaction{AssignedTo: me.PubKey, TableName: []byte{1}}
	otherTx := &Transaction{AssignedTo: me.PubKey, TableName: []byte{2}}
	err = db.WriteTransaction(tx.toDBTransaction())
	assert.Nil(t, err)
	err = db.WriteTransaction(otherTx.toDBTransaction())
	assert.Nil(t, err)

	bc := NewBlockchain(db, me, nil)

	err = bc.DeleteTransactions([]*Transaction{otherTx})
	assert.Nil(t, err)

	txs, err := db.GetAssignedTransactions(me.PubKey)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, tx, fromDBTransaction(txs[0]))
}

func TestBuildBlock(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	me := &Node{PubKey: []byte{69}}
	other := &Node{PubKey: []byte{70}}
	tx := &Transaction{TableName: []byte{1}}
	err = db.WriteTransaction(tx.toDBTransaction())
	assert.Nil(t, err)

	bc := NewBlockchain(db, me, []*Node{other})

	txs := []*Transaction{tx}
	b, err := bc.BuildBlock(txs)
	assert.Nil(t, err)
	assert.Equal(t, txs, b.Transactions)
	assert.Equal(t, me.PubKey, b.Creator)
	assert.Equal(t, [][]byte{other.PubKey}, b.Voters)
	assertRecent(t, b.CreatedAt.Int64())
}

func TestWriteBlock(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	b := &Block{Transactions: []*Transaction{&Transaction{TableName: []byte{123}}}}

	bc := NewBlockchain(db, nil, nil)

	err = bc.WriteBlock(b)
	assert.Nil(t, err)

	bs, err := db.GetBlocks([][]byte{b.Hash().Bytes()})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(bs))
	assert.Equal(t, b, fromDBBlock(bs[0]))
}

func TestGetBlocks(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	b := &Block{Transactions: []*Transaction{&Transaction{TableName: []byte{123}}}}
	err = db.WriteBlock(b.toDBBlock())
	assert.Nil(t, err)

	bc := NewBlockchain(db, nil, nil)

	bs, err := bc.GetBlocks([]Hash{b.Hash()})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(bs))
	assert.Equal(t, b, bs[0])
}

func TestGetOldestBlocks(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	b := &Block{
		CreatedAt:    big.NewInt(common.Now() - 3000),
		Transactions: []*Transaction{&Transaction{TableName: []byte{123}}},
	}
	otherB := &Block{
		CreatedAt:    big.NewInt(common.Now() - 2000),
		Transactions: []*Transaction{&Transaction{TableName: []byte{124}}},
	}
	otherB2 := &Block{
		CreatedAt:    big.NewInt(common.Now() - 1000),
		Transactions: []*Transaction{&Transaction{TableName: []byte{125}}},
	}
	err = db.WriteBlock(b.toDBBlock())
	assert.Nil(t, err)
	err = db.WriteBlock(otherB.toDBBlock())
	assert.Nil(t, err)
	err = db.WriteBlock(otherB2.toDBBlock())
	assert.Nil(t, err)

	bc := NewBlockchain(db, nil, nil)

	bs, err := bc.GetOldestBlocks(common.Now()-2500, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(bs))
	assert.Equal(t, otherB, bs[0])
	assert.Equal(t, otherB2, bs[1])
}

func TestBuildVote(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	var (
		me          = &Node{PubKey: []byte{69}}
		blockId     = BytesToHash([]byte("next"))
		prevBlockId = BytesToHash([]byte("prev"))
		value       = true
	)

	bc := NewBlockchain(db, me, nil)

	v, err := bc.BuildVote(blockId, prevBlockId, value)
	assert.Nil(t, err)
	assert.Equal(t, me.PubKey, v.Voter)
	assert.Equal(t, blockId, v.NextBlock)
	assert.Equal(t, prevBlockId, v.PrevBlock)
	assert.Equal(t, value, v.Value)
	assertRecent(t, v.VotedAt.Int64())
}

func TestWriteVote(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	var (
		me = &Node{PubKey: []byte{69}}
		ts = big.NewInt(69)
		v  = &Vote{
			PrevBlock: StringToHash("1"),
			NextBlock: StringToHash("2"),
			VotedAt:   ts,
			Voter:     me.PubKey,
		}
	)
	err = db.WriteVote(v.toDBVote())
	assert.Nil(t, err)

	bc := NewBlockchain(db, me, nil)

	err = bc.WriteVote(v)
	assert.Nil(t, err)

	vs, err := db.GetVotes(me.PubKey, ts.Int64())
	assert.Equal(t, 1, len(vs))
	assert.Equal(t, v, fromDBVote(vs[0]))
}

func TestGetRecentVotesMultiple(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	var (
		me = &Node{PubKey: []byte{69}}
		ts = big.NewInt(69)
		v1 = &Vote{
			PrevBlock: StringToHash("1"),
			NextBlock: StringToHash("2"),
			VotedAt:   ts,
			Voter:     me.PubKey,
		}
		v2 = &Vote{
			PrevBlock: StringToHash("2"),
			NextBlock: StringToHash("3"),
			VotedAt:   ts,
			Voter:     me.PubKey,
		}
		v3 = &Vote{
			PrevBlock: StringToHash("3"),
			NextBlock: StringToHash("4"),
			VotedAt:   ts,
			Voter:     me.PubKey,
		}
		v4 = &Vote{
			PrevBlock: StringToHash("3"),
			NextBlock: StringToHash("4"),
			VotedAt:   ts,
			Voter:     []byte{70},
		}
	)
	err = db.WriteVote(v1.toDBVote())
	assert.Nil(t, err)
	err = db.WriteVote(v2.toDBVote())
	assert.Nil(t, err)
	err = db.WriteVote(v3.toDBVote())
	assert.Nil(t, err)
	err = db.WriteVote(v4.toDBVote())
	assert.Nil(t, err)

	bc := NewBlockchain(db, me, nil)

	vs, err := bc.GetRecentVotes()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(vs))
	expected := []*Vote{v1, v2, v3}
	assert.Subset(t, expected, vs)
	assert.Subset(t, vs, expected)
}

func TestGetRecentVotesSingle(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	var (
		me = &Node{PubKey: []byte{69}}
		v1 = &Vote{
			PrevBlock: StringToHash("1"),
			NextBlock: StringToHash("2"),
			VotedAt:   big.NewInt(69),
			Voter:     me.PubKey,
		}
		v2 = &Vote{
			PrevBlock: StringToHash("2"),
			NextBlock: StringToHash("3"),
			VotedAt:   big.NewInt(10),
			Voter:     me.PubKey,
		}
	)
	err = db.WriteVote(v1.toDBVote())
	assert.Nil(t, err)
	err = db.WriteVote(v2.toDBVote())
	assert.Nil(t, err)

	bc := NewBlockchain(db, me, nil)

	vs, err := bc.GetRecentVotes()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(vs))
	assert.Equal(t, v1, vs[0])
}

func TestGetRecentVotesEmpty(t *testing.T) {
	db, err := meddb.NewMemoryBlockchainDB()
	assert.Nil(t, err)

	me := &Node{PubKey: []byte{69}}

	bc := NewBlockchain(db, me, nil)

	vs, err := bc.GetRecentVotes()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vs))
}

func TestRandomAssignee(t *testing.T) {
	node := &Node{PubKey: []byte{42}}
	otherNode := &Node{PubKey: []byte{43}}

	bc := &Blockchain{federation: []*Node{node, otherNode}}
	randNode := bc.randomAssignee(0)
	assert.Equal(t, node, randNode)
	randNode = bc.randomAssignee(1)
	assert.Equal(t, otherNode, randNode)
}
