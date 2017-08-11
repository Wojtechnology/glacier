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
	o, err := NewOutputFromMap(OUTPUT_TYPE_TABLE_EXISTS, map[string]interface{}{
		"TableName": tableName,
	})
	assert.Nil(t, err)
	typedO, ok := o.(*TableExistsOutput)
	assert.True(t, ok)
	assert.Equal(t, tableName, typedO.TableName)
}

func TestNewOutputFromMapInvalidField(t *testing.T) {
	_, err := NewOutputFromMap(OUTPUT_TYPE_TABLE_EXISTS, map[string]interface{}{
		"RowId": []byte("brah"),
	})
	assert.IsType(t, errors.New(""), err)
}

func TestNewOutputFromMapInvalidType(t *testing.T) {
	_, err := NewOutputFromMap(OUTPUT_TYPE_TABLE_EXISTS, map[string]interface{}{
		"TableName": "wrong type",
	})
	assert.IsType(t, errors.New(""), err)
}
