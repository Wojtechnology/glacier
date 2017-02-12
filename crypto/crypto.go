package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"math/big"
)

func CreateKey() (priv *ecdsa.PrivateKey, err error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
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

func SerializePrivateKey(priv *ecdsa.PrivateKey) ([]byte, error) {
	return x509.MarshalECPrivateKey(priv)
}

func SerializePublicKey(pub *ecdsa.PublicKey) ([]byte, error) {
	return x509.MarshalPKIXPublicKey(pub)
}
