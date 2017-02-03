package crypt

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"math/big"
)

func CreateKey() (priv *ecdsa.PrivateKey, err error) {
	priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	return priv, nil
}

func Sign(hash []byte, priv *ecdsa.PrivateKey) (r, s *big.Int, err error) {
	zero := big.NewInt(0)
	r, s, err = ecdsa.Sign(rand.Reader, priv, hash)
	if err != nil {
		return zero, zero, err
	}
	return r, s, nil
}

func Verify(hash []byte, pub *ecdsa.PublicKey, r, s *big.Int) (result bool) {
	return ecdsa.Verify(pub, hash, r, s)
}
