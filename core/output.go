package core

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/wojtechnology/glacier/meddb"
)

type OutputType int

// Defines OutputType "enum"
const (
	OUTPUT_TYPE_TABLE_EXISTS OutputType = iota // TABLE_EXISTS = 0 - unique table names
)

type Output interface {
	Type() OutputType
	Data() []byte
}

// --------------------------------
// TableExistsOutput implementation
//
// Used to check whether a table with a given name already exists.
// --------------------------------

type TableExistsOutput struct {
	TableName []byte
}

func (o *TableExistsOutput) Type() OutputType {
	return OUTPUT_TYPE_TABLE_EXISTS
}

func (o *TableExistsOutput) Data() []byte {
	return o.TableName
}

// -------
// Helpers
// -------

// Object used to rlpEncode and hash an output.
// Type field provides coverage for conflicts between different output types.
type outputHashObject struct {
	Type *big.Int
	Data []byte
}

// Hashes rlp encoded outputHashObject with fields filled in.
func hashOutput(o Output) Hash {
	return rlpHash(&outputHashObject{
		Type: intToBigInt(int(o.Type())),
		Data: o.Data(),
	})
}

// Mapper from core Output implementation to db Output object.
func toDBOutput(o Output) *meddb.Output {
	return &meddb.Output{
		Hash: hashOutput(o).Bytes(),
		Type: int(o.Type()),
		Data: o.Data(),
	}
}

// Mapper from db Output object to the appropriate core Output implementation.
func fromDBOutput(o *meddb.Output) (Output, error) {
	switch OutputType(o.Type) {
	case OUTPUT_TYPE_TABLE_EXISTS:
		// Maybe have some other interface for this
		return &TableExistsOutput{TableName: o.Data}, nil
	default:
		return nil, errors.New(fmt.Sprintf("Invalid output type %d\n", o.Type))
	}
}
