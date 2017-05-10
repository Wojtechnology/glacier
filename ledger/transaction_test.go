package ledger

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/crypto"
	"github.com/wojtechnology/glacier/test"
)

// Tests both SignTx as well as From functions
func TestSignTxFrom(t *testing.T) {
	priv, err := crypto.NewPrivateKey()
	assert.Nil(t, err)

	tx := &Transaction{To: []byte("fam"), Amount: big.NewInt(1000000)}
	signedTx, err := SignTx(tx, priv)
	assert.Nil(t, err)
	test.AssertEqual(t, tx.To, signedTx.To)
	test.AssertEqual(t, tx.Amount, signedTx.Amount)

	expectedFrom := AddressFromPubKey(crypto.MarshalPublicKey(&priv.PublicKey))
	actualFrom, err := signedTx.From()
	assert.Nil(t, err)
	test.AssertEqual(t, expectedFrom, actualFrom)
}
