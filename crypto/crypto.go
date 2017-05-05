package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"fmt"
	"golang.org/x/crypto/ripemd160"
	"golang.org/x/crypto/sha3"
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

func Verify(hash, pub []byte, r, s *big.Int) (result bool) {
	pubKey, err := ParsePublicKey(pub)
	if err != nil {
		return false
	}
	return ecdsa.Verify(pubKey, hash, r, s)
}

func MarshalPrivateKey(priv *ecdsa.PrivateKey) ([]byte, error) {
	return x509.MarshalECPrivateKey(priv)
}

func MarshalPublicKey(pub *ecdsa.PublicKey) ([]byte, error) {
	return x509.MarshalPKIXPublicKey(pub)
}

func ParsePrivateKey(priv []byte) (*ecdsa.PrivateKey, error) {
	return x509.ParseECPrivateKey(priv)
}

func ParsePublicKey(pub []byte) (*ecdsa.PublicKey, error) {
	pubKey, err := x509.ParsePKIXPublicKey(pub)
	if err != nil {
		return nil, err
	}
	if pubKey, ok := pubKey.(*ecdsa.PublicKey); ok {
		return pubKey, nil
	}
	return nil, errors.New(fmt.Sprintf("Invalid type \"%T\" of public key \"%v\"\n", pubKey, pub))
}

// SHA256 + RIPEMD160
func Hash160(s []byte) [20]byte {
	sha := sha3.New256()
	rip := ripemd160.New()

	var (
		hashed []byte = make([]byte, sha.Size())
		res    [20]byte
	)
	sha.Write(s)
	sha.Sum(hashed[:0])
	rip.Write(hashed)
	rip.Sum(res[:0])

	return res
}
