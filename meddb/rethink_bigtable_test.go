package meddb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	r "gopkg.in/gorethink/gorethink.v3"
)

const rethinkTableName = "HELLO"

func init() {
	session, err := r.Connect(r.ConnectOpts{Addresses: []string{"127.0.0.1"}})
	if err != nil {
		panic(err)
	}
	r.DBDrop("test").Run(session)
	r.DBCreate("test").Run(session)
	r.DB("test").TableCreate(rethinkTableName).RunWrite(session)
	r.DB("test").Table(rethinkTableName).IndexCreate("row_id").RunWrite(session)
	r.DB("test").Table(rethinkTableName).IndexWait().Run(session)
}

func TestRethinkPutGet(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	defer rethinkClearTable(bt, rethinkTableName)
	testPutGet(t, bt, []byte(rethinkTableName))
}

func TestRethinkPutGetEmpty(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	defer rethinkClearTable(bt, rethinkTableName)
	testPutGetEmpty(t, bt, []byte(rethinkTableName))
}

func TestRethinkPutGetVer(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	defer rethinkClearTable(bt, rethinkTableName)
	testPutGetVer(t, bt, []byte(rethinkTableName))
}

func TestRethinkPutOverwrite(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	defer rethinkClearTable(bt, rethinkTableName)
	testPutOverwrite(t, bt, []byte(rethinkTableName))
}

func TestRethinkGetExact(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	defer rethinkClearTable(bt, rethinkTableName)
	testGetExact(t, bt, []byte(rethinkTableName))
}

func TestRethinkGetLimit(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	defer rethinkClearTable(bt, rethinkTableName)
	testGetLimit(t, bt, []byte(rethinkTableName))
}

func TestRethinkGetRange(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	defer rethinkClearTable(bt, rethinkTableName)
	testGetRange(t, bt, []byte(rethinkTableName))
}

func TestRethinkGetTableNotFound(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	testGetTableNotFound(t, bt)
}

func TestRethinkPutTableNotFound(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	testPutTableNotFound(t, bt)
}

func TestRethinkCreateTableAlreadyExists(t *testing.T) {
	bt, err := NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	assert.Nil(t, err)
	testCreateTableAlreadyExists(t, bt)
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

func rethinkClearTable(bt *RethinkBigtable, tableName string) {
	r.DB("test").Table(tableName).Delete().Run(bt.session)
}
