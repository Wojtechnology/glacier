package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// -----
// Tests
// -----

func TestNewOutputFromMap(t *testing.T) {
	tableName := []byte("yo")
	o, err := NewOutputFromMap(OUTPUT_TYPE_TABLE_EXISTS, map[string][]byte{
		"TableName": tableName,
	})
	assert.Nil(t, err)
	typedO, ok := o.(*TableExistsOutput)
	assert.True(t, ok)
	assert.Equal(t, tableName, typedO.TableName())
}

func TestNewOutputFromMapInvalidField(t *testing.T) {
	_, err := NewOutputFromMap(OUTPUT_TYPE_TABLE_EXISTS, map[string][]byte{
		"RowId": []byte("brah"),
	})
	assert.IsType(t, errors.New(""), err)
}
