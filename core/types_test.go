package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesHash(t *testing.T) {
	b := []byte("hello world fam")

	// Add padding to expected
	var expected [HashLength]byte
	copy(expected[HashLength-len(b):], b)

	assert.Equal(t, expected[:], BytesToHash(b).Bytes())
}

func TestStringHash(t *testing.T) {
	s := "hello world fam"

	// Add padding to expected
	var expectedB [HashLength]byte
	copy(expectedB[HashLength-len(s):], []byte(s))
	expected := string(expectedB[:])

	assert.Equal(t, expected, StringToHash(s).String())
}

func TestBigHash(t *testing.T) {
	i := big.NewInt(100)
	assert.Equal(t, i, BigToHash(i).Big())
}
