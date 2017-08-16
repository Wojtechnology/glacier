package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// -----
// Tests
// -----

func TestNewInputFromMap(t *testing.T) {
	tableName := []byte("yo")
	o, err := NewInputFromMap(INPUT_TYPE_ADMIN, []byte("helo"), map[string][]byte{
		"Sig": tableName,
	})
	assert.Nil(t, err)
	typedO, ok := o.(*AdminInput)
	assert.True(t, ok)
	assert.Equal(t, tableName, typedO.Sig)
}

func TestNewInputFromMapInvalidField(t *testing.T) {
	_, err := NewInputFromMap(INPUT_TYPE_ADMIN, []byte("helo"), map[string][]byte{
		"RowId": []byte("brah"),
	})
	assert.IsType(t, errors.New(""), err)
}
