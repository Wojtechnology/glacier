package ledger

import (
	"math/big"
	"testing"

	"github.com/wojtechnology/medblocks/test"
)

func TestBytesHash(t *testing.T) {
	b := []byte("hello world fam")

	// Add padding to expected
	var expected [HashLength]byte
	copy(expected[HashLength-len(b):], b)

	test.AssertBytesEqual(t, expected[:], BytesToHash(b).Bytes())
}

func TestStringHash(t *testing.T) {
	s := "hello world fam"

	// Add padding to expected
	var expectedB [HashLength]byte
	copy(expectedB[HashLength-len(s):], []byte(s))
	expected := string(expectedB[:])

	test.AssertEqual(t, expected, StringToHash(s).String())
}

func TestBigHash(t *testing.T) {
	i := big.NewInt(100)
	test.AssertEqual(t, i, BigToHash(i).Big())
}

func TestBytesAddress(t *testing.T) {
	b := []byte("hello world fam")

	// Add padding to expected
	var expected [AddressLength]byte
	copy(expected[AddressLength-len(b):], b)

	test.AssertBytesEqual(t, expected[:], BytesToAddress(b).Bytes())
}

func TestStringAddress(t *testing.T) {
	s := "hello world fam"

	// Add padding to expected
	var expectedB [AddressLength]byte
	copy(expectedB[AddressLength-len(s):], []byte(s))
	expected := string(expectedB[:])

	test.AssertEqual(t, expected, StringToAddress(s).String())
}

func TestBigAddress(t *testing.T) {
	i := big.NewInt(100)
	test.AssertEqual(t, i, BigToAddress(i).Big())
}

func TestEncodeDecodeNonce(t *testing.T) {
	var i uint64 = 42
	test.AssertEqual(t, i, EncodeNonce(i).Uint64())
}
