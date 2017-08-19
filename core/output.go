package core

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/wojtechnology/glacier/common"
	"github.com/wojtechnology/glacier/meddb"
)

type OutputType int

// Defines OutputType "enum"
const (
	OUTPUT_TYPE_TABLE_EXISTS     OutputType = iota // TABLE_EXISTS     = 0
	OUTPUT_TYPE_COL_ALLOWED                        // COL_ALLOWED      = 1
	OUTPUT_TYPE_ALL_COLS_ALLOWED                   // ALL_COLS_ALLOWED = 2
	OUTPUT_TYPE_ALL_ADMINS                         // ALL_ADMINS       = 3
	OUTPUT_TYPE_ADMIN                              // ADMIN            = 4
	OUTPUT_TYPE_ALL_WRITERS                        // ALL_WRITERS      = 5
	OUTPUT_TYPE_WRITER                             // WRITER           = 6
	OUTPUT_TYPE_ALL_ROW_WRITERS                    // ALL_ROW_WRITERS  = 7
	OUTPUT_TYPE_ROW_WRITER                         // ROW_WRITER       = 8
)

type Output interface {
	Type() OutputType
	Data() []byte
	FromData([]byte) error
	TableName() []byte
	SetTableName([]byte)
}

type TableNameMixin struct {
	Table []byte
}

func (mixin *TableNameMixin) TableName() []byte {
	return mixin.Table
}

func (mixin *TableNameMixin) SetTableName(tableName []byte) {
	mixin.Table = tableName
}

// --------------------------------
// TableExistsOutput implementation
//
// Used to check whether a table with a given name already exists.
// --------------------------------

type TableExistsOutput struct {
	*TableNameMixin
}

func (o *TableExistsOutput) Type() OutputType {
	return OUTPUT_TYPE_TABLE_EXISTS
}

func (o *TableExistsOutput) Data() []byte {
	return o.TableName()
}

func (o *TableExistsOutput) FromData(data []byte) error {
	o.SetTableName(data)
	return nil
}

// --------------------------------
// ColumnAllowedOutput implementation
//
// Used to check if you can write to the given column in the table
// --------------------------------

type ColAllowedOutput struct {
	*TableNameMixin
	ColName []byte
}

func (o *ColAllowedOutput) Type() OutputType {
	return OUTPUT_TYPE_COL_ALLOWED
}

func (o *ColAllowedOutput) Data() []byte {
	// TODO: Log on error here, should never happen
	data, _ := rlpEncode(o)
	return data
}

func (o *ColAllowedOutput) FromData(data []byte) error {
	if err := rlpDecode(data, o); err != nil {
		return err
	}
	return nil
}

// --------------------------------
// AllColsAllowedOutput implementation
//
// Used to check if you can write to the given column in the table
// --------------------------------

type AllColsAllowedOutput struct {
	*TableNameMixin
}

func (o *AllColsAllowedOutput) Type() OutputType {
	return OUTPUT_TYPE_ALL_COLS_ALLOWED
}

func (o *AllColsAllowedOutput) Data() []byte {
	return o.TableName()
}

func (o *AllColsAllowedOutput) FromData(data []byte) error {
	o.SetTableName(data)
	return nil
}

// --------------------------------
// AllAdminsOutput implementation
//
// Signals that all users can update this table
// --------------------------------

type AllAdminsOutput struct {
	*TableNameMixin
}

func (o *AllAdminsOutput) Type() OutputType {
	return OUTPUT_TYPE_ALL_ADMINS
}

func (o *AllAdminsOutput) Data() []byte {
	return o.TableName()
}

func (o *AllAdminsOutput) FromData(data []byte) error {
	o.SetTableName(data)
	return nil
}

// --------------------------------
// AdminOutput implementation
//
// Allows a particular user to update a table
// --------------------------------

type AdminOutput struct {
	*TableNameMixin
	PubKey []byte
}

func (o *AdminOutput) Type() OutputType {
	return OUTPUT_TYPE_ADMIN
}

func (o *AdminOutput) Data() []byte {
	// TODO: Log on error here, should never happen
	data, _ := rlpEncode(o)
	return data
}

func (o *AdminOutput) FromData(data []byte) error {
	if err := rlpDecode(data, o); err != nil {
		return err
	}
	return nil
}

// --------------------------------
// AllWritersOutput implementation
//
// Signals that all users can write to this table
// --------------------------------

type AllWritersOutput struct {
	*TableNameMixin
}

func (o *AllWritersOutput) Type() OutputType {
	return OUTPUT_TYPE_ALL_WRITERS
}

func (o *AllWritersOutput) Data() []byte {
	return o.TableName()
}

func (o *AllWritersOutput) Requirement() OutputRequirement {
	return OUTPUT_REQUIREMENT_NONE
}

func (o *AllWritersOutput) FromData(data []byte) error {
	o.SetTableName(data)
	return nil
}

// --------------------------------
// WriterOutput implementation
//
// Allows a particular user to write to a table
// --------------------------------

type WriterOutput struct {
	*TableNameMixin
	PubKey []byte
}

func (o *WriterOutput) Type() OutputType {
	return OUTPUT_TYPE_WRITER
}

func (o *WriterOutput) Data() []byte {
	// TODO: Log on error here, should never happen
	data, _ := rlpEncode(o)
	return data
}

func (o *WriterOutput) Requirement() OutputRequirement {
	return OUTPUT_REQUIREMENT_REQUIRED
}

func (o *WriterOutput) FromData(data []byte) error {
	if err := rlpDecode(data, o); err != nil {
		return err
	}
	return nil
}

// --------------------------------
// AllRowWritersOutput implementation
//
// Allows any user to write to the particular row
// --------------------------------

type AllRowWritersOutput struct {
	*TableNameMixin
	RowId []byte
}

func (o *AllRowWritersOutput) Type() OutputType {
	return OUTPUT_TYPE_ALL_ROW_WRITERS
}

func (o *AllRowWritersOutput) Data() []byte {
	// TODO: Log on error here, should never happen
	data, _ := rlpEncode(o)
	return data
}

func (o *AllRowWritersOutput) Requirement() OutputRequirement {
	return OUTPUT_REQUIREMENT_NONE
}

func (o *AllRowWritersOutput) FromData(data []byte) error {
	if err := rlpDecode(data, o); err != nil {
		return err
	}
	return nil
}

// --------------------------------
// RowWriterOutput implementation
//
// Allows any user to write to the particular row
// --------------------------------

type RowWriterOutput struct {
	*TableNameMixin
	RowId  []byte
	PubKey []byte
}

func (o *RowWriterOutput) Type() OutputType {
	return OUTPUT_TYPE_ROW_WRITER
}

func (o *RowWriterOutput) Data() []byte {
	// TODO: Log on error here, should never happen
	data, _ := rlpEncode(o)
	return data
}

func (o *RowWriterOutput) Requirement() OutputRequirement {
	return OUTPUT_REQUIREMENT_REQUIRED
}

func (o *RowWriterOutput) FromData(data []byte) error {
	if err := rlpDecode(data, o); err != nil {
		return err
	}
	return nil
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
func HashOutput(o Output) Hash {
	return rlpHash(&outputHashObject{
		Type: intToBigInt(int(o.Type())),
		Data: o.Data(),
	})
}

// Mapper from core Output implementation to db Output object.
func toDBOutput(o Output) *meddb.Output {
	return &meddb.Output{
		Hash: HashOutput(o).Bytes(),
		Type: int(o.Type()),
		Data: o.Data(),
	}
}

// Returns instance of correct output implementation for given `outputType`.
func outputFromOutputType(outputType OutputType) (Output, error) {
	switch outputType {
	case OUTPUT_TYPE_TABLE_EXISTS:
		return &TableExistsOutput{TableNameMixin: &TableNameMixin{}}, nil
	case OUTPUT_TYPE_COL_ALLOWED:
		return &ColAllowedOutput{TableNameMixin: &TableNameMixin{}}, nil
	case OUTPUT_TYPE_ALL_COLS_ALLOWED:
		return &AllColsAllowedOutput{TableNameMixin: &TableNameMixin{}}, nil
	case OUTPUT_TYPE_ALL_ADMINS:
		return &AllAdminsOutput{TableNameMixin: &TableNameMixin{}}, nil
	case OUTPUT_TYPE_ADMIN:
		return &AdminOutput{TableNameMixin: &TableNameMixin{}}, nil
	case OUTPUT_TYPE_ALL_WRITERS:
		return &AllWritersOutput{TableNameMixin: &TableNameMixin{}}, nil
	case OUTPUT_TYPE_WRITER:
		return &WriterOutput{TableNameMixin: &TableNameMixin{}}, nil
	case OUTPUT_TYPE_ALL_ROW_WRITERS:
		return &AllRowWritersOutput{TableNameMixin: &TableNameMixin{}}, nil
	case OUTPUT_TYPE_ROW_WRITER:
		return &RowWriterOutput{TableNameMixin: &TableNameMixin{}}, nil
	default:
		return nil, errors.New(fmt.Sprintf("Invalid output type %d\n", outputType))
	}
}

func NewOutputFromMap(outputType OutputType, data map[string][]byte) (Output, error) {
	coreOutput, err := outputFromOutputType(outputType)
	if err != nil {
		return nil, err
	}
	for fieldName, fieldValue := range data {
		if fieldName == "TableName" {
			coreOutput.SetTableName(fieldValue)
		} else {
			if err := common.SetStructField(coreOutput, fieldName, fieldValue); err != nil {
				return nil, err
			}
		}
	}

	return coreOutput, nil
}

// Factory method for creating outputs
func NewOutput(outputType OutputType, data []byte) (Output, error) {
	coreOutput, err := outputFromOutputType(outputType)
	if err != nil {
		return nil, err
	}
	if err := coreOutput.FromData(data); err != nil {
		return nil, err
	}
	return coreOutput, nil
}
