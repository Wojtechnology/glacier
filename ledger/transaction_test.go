package ledger

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
	"github.com/wojtechnology/glacier/test"
)

func TestWriteReadUnspentTxOutput(t *testing.T) {
	o := &TxOutput{
		Cubes:  big.NewInt(12),
		PubKey: []byte("YOLO LOL"),
	}
	db, _ := meddb.NewMemoryDatabase()

	err := o.WriteUnspent(db)
	assert.Nil(t, err)

	var newO *TxOutput
	newO, err = GetUnspentTxOutput(db, o.Hash())

	assert.Nil(t, err)
	test.AssertEqual(t, o, newO)
}

func TestDeleteUnspentTxOutput(t *testing.T) {
	o := &TxOutput{
		Cubes:  big.NewInt(12),
		PubKey: []byte("YOLO LOL"),
	}
	db, _ := meddb.NewMemoryDatabase()
	key := buildUnspentTxOutputsKey(o.Hash())

	err := o.WriteUnspent(db)
	assert.Nil(t, err)

	var exists bool
	exists, err = db.Contains(key)

	assert.Nil(t, err)
	assert.True(t, exists)

	err = o.DeleteUnspent(db)
	assert.Nil(t, err)

	exists, err = db.Contains(key)

	assert.Nil(t, err)
	assert.False(t, exists)
}

func TestDeleteUnspentTxOutputDoesNotExist(t *testing.T) {
	o := &TxOutput{
		Cubes:  big.NewInt(12),
		PubKey: []byte("YOLO LOL"),
	}
	db, _ := meddb.NewMemoryDatabase()

	err := o.DeleteUnspent(db)
	assert.IsType(t, meddb.NotFoundError{}, err)
}
