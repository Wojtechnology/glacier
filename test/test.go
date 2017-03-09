package test

import (
	"bytes"
	"reflect"
	"testing"
)

func AssertEqual(t *testing.T, a, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Errorf("%v != %v", a, b)
	}
}

func AssertBytesEqual(t *testing.T, a, b []byte) {
	if !bytes.Equal(a, b) {
		t.Errorf("%v != %v", a, b)
	}
}
