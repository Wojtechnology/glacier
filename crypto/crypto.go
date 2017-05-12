package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"golang.org/x/crypto/ripemd160"
	"golang.org/x/crypto/sha3"
	"math/big"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

// Taken from https://github.com/ethereum/go-ethereum/blob/master/common/math/big.go
const (
	// number of bits in a big.Word
	wordBits = 32 << (uint64(^big.Word(0)) >> 63)
	// number of bytes in a big.Word
	wordBytes = wordBits / 8
)

func S256() elliptic.Curve {
	return secp256k1.S256()
}

func NewPrivateKey() (priv *ecdsa.PrivateKey, err error) {
	return ecdsa.GenerateKey(S256(), rand.Reader)
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
	privBytes := paddedBigIntBytes(priv.D, priv.Params().BitSize/8)
	defer zeroBytes(privBytes)
	return secp256k1.Sign(hash, privBytes)
}

func paddedBigIntBytes(b *big.Int, n int) []byte {
	if b.BitLen()/8 >= n {
		return b.Bytes()
	}
	ret := make([]byte, n)
	i := n
	for _, word := range b.Bits() {
		for j := 0; j < wordBytes && i > 0; j++ {
			i--
			ret[i] = byte(word)
			word >>= 8
		}
	}
	return ret
}

func zeroBytes(b []byte) {
	for i, _ := range b {
		b[i] = 0
	}
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
