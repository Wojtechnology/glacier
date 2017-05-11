package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
	"github.com/wojtechnology/glacier/test"
)

func TestReadWriteAccount(t *testing.T) {
	a, _, err := NewAccount()
	assert.Nil(t, err)
	db, err := meddb.NewMemoryDatabase()
	assert.Nil(t, err)

	err = a.Write(db)
	assert.Nil(t, err)
	newA, err := GetAccount(db, a.Addr)
	assert.Nil(t, err)
	test.AssertEqual(t, a, newA)
}
