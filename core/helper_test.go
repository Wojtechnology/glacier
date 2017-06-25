package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testInnerObj struct {
	SomeInnerByteString []byte
}

type testObj struct {
	SomeBigInt     *big.Int
	SomeString     string
	SomeByteString []byte
	SomeInnerObj   *testInnerObj
}

func TestRlpEncodeDecode(t *testing.T) {
	o := &testObj{
		SomeBigInt:     big.NewInt(69),
		SomeString:     "Sad friday nights",
		SomeByteString: []byte("Time to get better"),
		SomeInnerObj: &testInnerObj{
			SomeInnerByteString: []byte("uhhh"),
		},
	}

	b, err := rlpEncode(o)
	assert.Nil(t, err)

	newO := new(testObj)
	err = rlpDecode(b, newO)
	assert.Nil(t, err)
	assert.Equal(t, o, newO)
}

func TestIntBigIntMapper(t *testing.T) {
	assert.Equal(t, 69, bigIntToInt(intToBigInt(69)))
}
