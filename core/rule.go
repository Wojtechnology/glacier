package core

import (
	"errors"
	"fmt"
)

type Rule interface {
	RequiredOutputIds(*Transaction) [][]byte
	Validate(*Transaction, map[string]Output, map[string][]Input) error
}

// --------------------------------
// TableExistsOutputMixin
// --------------------------------

type TableExistsOutputMixin struct{}

func (mixin *TableExistsOutputMixin) getTableExistsOutputHash(tx *Transaction) Hash {
	return hashOutput(&TableExistsOutput{TableName: tx.TableName})
}

func (mixin *TableExistsOutputMixin) RequiredOutputIds(tx *Transaction) [][]byte {
	return [][]byte{mixin.getTableExistsOutputHash(tx).Bytes()}
}

// --------------------------------
// TableExistsRule implementation
//
// Used to check whether a table with a given name already exists.
// --------------------------------

type TableExistsRule struct {
	TableExistsOutputMixin
}

func (rule *TableExistsRule) Validate(tx *Transaction, linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	if _, ok := linkedOutputs[rule.getTableExistsOutputHash(tx).String()]; !ok {
		return errors.New(fmt.Sprintf("Table does not exist: %v\n", tx.TableName))
	}
	return nil
}

// --------------------------------
// TableMissingRule implementation
//
// Used to check whether a table with a given name does not exist.
// --------------------------------

type TableMissingRule struct {
	TableExistsOutputMixin
}

func (rule *TableMissingRule) Validate(tx *Transaction, linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	if _, ok := linkedOutputs[rule.getTableExistsOutputHash(tx).String()]; ok {
		return errors.New(fmt.Sprintf("Table already exists: %v\n", tx.TableName))
	}
	return nil
}
