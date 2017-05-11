package ledger

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
	"github.com/wojtechnology/glacier/test"
)

func TestAddTransaction(t *testing.T) {
	db, err := meddb.NewMemoryDatabase()
	assert.Nil(t, err)

	fromAcc, fromPriv, err := NewAccount()
	assert.Nil(t, err)
	fromAcc.Balance = big.NewInt(10)
	err = fromAcc.Write(db)
	assert.Nil(t, err)

	toAcc, _, err := NewAccount()
	assert.Nil(t, err)
	toAcc.Balance = big.NewInt(5)
	err = toAcc.Write(db)
	assert.Nil(t, err)

	tx := &Transaction{To: toAcc.Addr, Amount: big.NewInt(6)}
	tx, err = SignTx(tx, fromPriv)
	assert.Nil(t, err)

	c, err := NewChain(db)
	assert.Nil(t, err)

	tx, err = c.AddTransaction(tx)
	assert.Nil(t, err)

	fromAcc, err = GetAccount(db, fromAcc.Addr)
	assert.Nil(t, err)
	toAcc, err = GetAccount(db, toAcc.Addr)
	assert.Nil(t, err)
	test.AssertEqual(t, 0, fromAcc.Balance.Cmp(big.NewInt(4)))
	test.AssertEqual(t, 0, toAcc.Balance.Cmp(big.NewInt(11)))

	block, err := GetHeadBlock(db)
	test.AssertEqual(t, 1, len(block.Body.Transactions))
	test.AssertEqual(t, tx.SigHash(), block.Body.Transactions[0].SigHash())
}
