package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	// "crypto/rand"
	"errors"
	"fmt"
	"golang.org/x/crypto/ripemd160"
	"golang.org/x/crypto/sha3"
	"math/big"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

func S256() elliptic.Curve {
	return secp256k1.S256()
}

func NewPrivateKey() (priv *ecdsa.PrivateKey, err error) {
	// return ecdsa.GenerateKey(S256(), rand.Reader)
	priv = new(ecdsa.PrivateKey)
	data := []byte{73, 59, 140, 154, 253, 185, 180, 197, 255, 114, 161, 26, 215, 228, 235, 29, 156, 34, 189, 69, 1, 58, 244, 243, 74, 22, 252, 13, 176, 112, 147, 8}
	priv.D = new(big.Int).SetBytes(data)
	priv.PublicKey.Curve = S256()
	priv.PublicKey.X, priv.PublicKey.Y = priv.PublicKey.Curve.ScalarBaseMult(data)
	return priv, nil
}

// Retrieves public key from signature used to sign hash
func RetrievePublicKey(hash, sig []byte) ([]byte, error) {
	return secp256k1.RecoverPubkey(hash, sig)
}

// Signs hash using given private key. Returns signature
func Sign(hash []byte, priv *ecdsa.PrivateKey) (sig []byte, err error) {
	if len(hash) != 32 {
		return nil, errors.New(fmt.Sprintf("Hash \"%v\" should be of length 32", hash))
	}
	return secp256k1.Sign(hash, priv.D.Bytes())
}

// Returns bytes representation of private key (code borrowed from ethereum project)
func MarshalPrivateKey(priv *ecdsa.PrivateKey) []byte {
	if priv == nil {
		return nil
	}
	return priv.D.Bytes()
}

// Returns bytes representation of public key (code borrowed from ethereum project)
func MarshalPublicKey(pub *ecdsa.PublicKey) []byte {
	if pub == nil || pub.X == nil || pub.Y == nil {
		return nil
	}
	return elliptic.Marshal(S256(), pub.X, pub.Y)
}

// Parses bytes representation of private key (code borrowed from ethereum project)
func ParsePrivateKey(data []byte) *ecdsa.PrivateKey {
	if len(data) == 0 {
		return nil
	}
	priv := new(ecdsa.PrivateKey)
	priv.PublicKey.Curve = S256()
	priv.D = new(big.Int).SetBytes(data)
	priv.PublicKey.X, priv.PublicKey.Y = priv.PublicKey.Curve.ScalarBaseMult(data)
	return priv
}

// Parses bytes representation of public key (code borrowed from ethereum project)
func ParsePublicKey(data []byte) *ecdsa.PublicKey {
	if len(data) == 0 {
		return nil
	}
	x, y := elliptic.Unmarshal(S256(), data)
	return &ecdsa.PublicKey{Curve: S256(), X: x, Y: y}
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
