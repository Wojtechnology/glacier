package meddb

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	r "gopkg.in/gorethink/gorethink.v3"
)

func TestRethinkCreateTableAlreadyExists(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	r.DBCreate("test").Run(bt.session)
	testCreateTableAlreadyExists(t, bt)
	r.DBDrop("test").Run(bt.session)
}

// ------------
// Test Helpers
// ------------

func TestInt64ToBytes(t *testing.T) {
	var x int64 = 42
	expected := []byte{128, 0, 0, 0, 0, 0, 0, 42}
	assert.Equal(t, expected, int64ToBytes(x))
}

func TestInt64ToBytesLargest(t *testing.T) {
	var x int64 = 9223372036854775807
	expected := []byte{255, 255, 255, 255, 255, 255, 255, 255}
	assert.Equal(t, expected, int64ToBytes(x))
}

func TestInt64ToBytesLargestNegative(t *testing.T) {
	var x int64 = -9223372036854775808
	expected := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, expected, int64ToBytes(x))
}

func TestInt64ToBytesNegative(t *testing.T) {
	var x int64 = -42
	expected := []byte{127, 255, 255, 255, 255, 255, 255, 214}
	assert.Equal(t, expected, int64ToBytes(x))
}

func TestInt64ToBytesZero(t *testing.T) {
	var x int64 = 0
	expected := []byte{128, 0, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, expected, int64ToBytes(x))
}

func TestBytesToInt64(t *testing.T) {
	var expected int64 = 42
	b := []byte{128, 0, 0, 0, 0, 0, 0, 42}
	assert.Equal(t, expected, bytesToInt64(b))
}

func TestBytesToInt64Largest(t *testing.T) {
	var expected int64 = 9223372036854775807
	b := []byte{255, 255, 255, 255, 255, 255, 255, 255}
	assert.Equal(t, expected, bytesToInt64(b))
}

func TestBytesToInt64LargestNegative(t *testing.T) {
	var expected int64 = -9223372036854775808
	b := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, expected, bytesToInt64(b))
}

func TestBytesToInt64Negative(t *testing.T) {
	var expected int64 = -42
	b := []byte{127, 255, 255, 255, 255, 255, 255, 214}
	assert.Equal(t, expected, bytesToInt64(b))
}

func TestBytesToInt64Zero(t *testing.T) {
	var expected int64 = 0
	b := []byte{128, 0, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, expected, bytesToInt64(b))
}
