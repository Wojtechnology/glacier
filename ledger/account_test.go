package ledger

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
	"github.com/wojtechnology/glacier/test"
)

func TestReadWriteAccount(t *testing.T) {
	a := &Account{
		Nonce:   AccountNonce(EncodeNonce(42)),
		Addr:    StringToAddress("home"),
		Balance: big.NewInt(41),
	}
	db, err := meddb.NewMemoryDatabase()
	assert.Nil(t, err)

	err = a.Write(db)
	assert.Nil(t, err)
	newA, err := GetAccount(db, a.Addr)
	assert.Nil(t, err)
	test.AssertEqual(t, a, newA)
}
