package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	SomeField []byte
}

func TestSetStructField(t *testing.T) {
	s := &testStruct{}
	val := []byte("yo")
	err := SetStructField(s, "SomeField", val)
	assert.Nil(t, err)
	assert.Equal(t, val, s.SomeField)
}

func TestSetStructFieldInvalidField(t *testing.T) {
	s := &testStruct{}
	err := SetStructField(s, "NotField", []byte("yo"))
	assert.IsType(t, errors.New(""), err)
}

func TestSetStructFieldInvalidType(t *testing.T) {
	s := &testStruct{}
	err := SetStructField(s, "SomeField", 32)
	assert.IsType(t, errors.New(""), err)
}
