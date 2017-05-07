package crypto

import (
	"crypto/ecdsa"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/test"
)

func TestSignRetrieve(t *testing.T) {
	priv, err := NewPrivateKey()
	assert.Nil(t, err)

	hash := make([]byte, 32)
	hash[4] = 2

	sig, err := Sign(hash, priv)
	assert.Nil(t, err)

	pub := priv.Public().(*ecdsa.PublicKey)
	expected := MarshalPublicKey(pub)

	actual, err := RetrievePublicKey(hash, sig)
	assert.Nil(t, err)
	test.AssertBytesEqual(t, expected, actual)
}

func TestMarshalParsePrivateKey(t *testing.T) {
	priv, err := NewPrivateKey()
	assert.Nil(t, err)

	data := MarshalPrivateKey(priv)
	newPriv := ParsePrivateKey(data)
	test.AssertEqual(t, priv, newPriv)
}

func TestMarshalParsePublicKey(t *testing.T) {
	priv, err := NewPrivateKey()
	assert.Nil(t, err)

	data := MarshalPublicKey(&priv.PublicKey)
	newPub := ParsePublicKey(data)
	test.AssertEqual(t, &priv.PublicKey, newPub)
}
