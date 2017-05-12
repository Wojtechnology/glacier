package ledger

import (
	"crypto/ecdsa"
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
	"github.com/wojtechnology/glacier/test"
)

func createAccounts(t *testing.T, db meddb.Database, fromBal, toBal int64) (*Account, *Account,
	*ecdsa.PrivateKey) {
	fromAcc, fromPriv, err := NewAccount()
	assert.Nil(t, err)
	fromAcc.Balance = big.NewInt(fromBal)
	err = fromAcc.Write(db)
	assert.Nil(t, err)

	toAcc, _, err := NewAccount()
	assert.Nil(t, err)
	toAcc.Balance = big.NewInt(toBal)
	err = toAcc.Write(db)
	assert.Nil(t, err)

	return fromAcc, toAcc, fromPriv
}

func createSignedTransaction(t *testing.T, fromPriv *ecdsa.PrivateKey,
	toAcc *Account, amount int64) *Transaction {
	tx := &Transaction{To: toAcc.Addr, Amount: big.NewInt(amount)}
	tx, err := SignTx(tx, fromPriv)
	assert.Nil(t, err)

	return tx
}

func assertBalances(t *testing.T, db meddb.Database,
	fromAddr, toAddr Address, fromBal, toBal int64) {

	fromAcc, err := GetAccount(db, fromAddr)
	assert.Nil(t, err)
	toAcc, err := GetAccount(db, toAddr)
	assert.Nil(t, err)
	test.AssertEqual(t, 0, fromAcc.Balance.Cmp(big.NewInt(fromBal)))
	test.AssertEqual(t, 0, toAcc.Balance.Cmp(big.NewInt(toBal)))
}

func TestAddTransaction(t *testing.T) {
	db, err := meddb.NewMemoryDatabase()
	assert.Nil(t, err)

	fromAcc, toAcc, fromPriv := createAccounts(t, db, 10, 5)
	tx := createSignedTransaction(t, fromPriv, toAcc, 6)

	c, err := NewChain(db)
	assert.Nil(t, err)

	tx, err = c.AddTransaction(tx)
	assert.Nil(t, err)

	assertBalances(t, db, fromAcc.Addr, toAcc.Addr, 4, 11)

	block, err := GetHeadBlock(db)
	assert.Nil(t, err)
	test.AssertEqual(t, 1, len(block.Body.Transactions))
	test.AssertEqual(t, tx.SigHash(), block.Body.Transactions[0].SigHash())
}

func TestAddTransactionInsufficient(t *testing.T) {
	db, err := meddb.NewMemoryDatabase()
	assert.Nil(t, err)

	fromAcc, toAcc, fromPriv := createAccounts(t, db, 5, 10)
	tx := createSignedTransaction(t, fromPriv, toAcc, 6)

	c, err := NewChain(db)
	assert.Nil(t, err)

	tx, err = c.AddTransaction(tx)
	assert.IsType(t, errors.New(""), err)

	assertBalances(t, db, fromAcc.Addr, toAcc.Addr, 5, 10)

	block, err := GetHeadBlock(db)
	assert.Nil(t, err)
	test.AssertEqual(t, 0, len(block.Body.Transactions))
}
