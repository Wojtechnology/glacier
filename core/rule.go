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

// --------------------------------
// ColAllowedRule implementation
//
// Used to check whether given columns are allowed for given table
// --------------------------------

type ColsAllowedRule struct{}

func (rule *ColsAllowedRule) getAllColsAllowedOutputHash(tx *Transaction) Hash {
	return hashOutput(&AllColsAllowedOutput{TableName: tx.TableName})
}

func (rule *ColsAllowedRule) getColAllowedOutputHashes(tx *Transaction) []Hash {
	hashes := make([]Hash, len(tx.Cols))

	i := 0
	for colId, _ := range tx.Cols {
		hashes[i] = hashOutput(&ColAllowedOutput{TableName: tx.TableName, ColName: []byte(colId)})
		i++
	}

	return hashes
}

func (rule *ColsAllowedRule) RequiredOutputIds(tx *Transaction) [][]byte {
	colAllowedHashes := rule.getColAllowedOutputHashes(tx)
	outputIds := make([][]byte, len(colAllowedHashes))

	for i, hash := range colAllowedHashes {
		outputIds[i] = hash.Bytes()
	}

	outputIds = append(outputIds, rule.getAllColsAllowedOutputHash(tx).Bytes())
	return outputIds
}

func (rule *ColsAllowedRule) Validate(tx *Transaction, linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	allColsHash := rule.getAllColsAllowedOutputHash(tx).String()
	if _, ok := linkedOutputs[allColsHash]; ok {
		// All columns are allowed on this table, let it slide
		return nil
	}

	// Otherwise check if all columns are allowed
	colAllowedHashes := rule.getColAllowedOutputHashes(tx)
	disallowedCols := make([][]byte, 0)

	for _, hash := range colAllowedHashes {
		if _, ok := linkedOutputs[hash.String()]; !ok {
			disallowedCols = append(disallowedCols, hash.Bytes())
		}
	}

	if len(disallowedCols) > 0 {
		return errors.New(fmt.Sprintf("Columns not allowed: %v\n", disallowedCols))
	}

	return nil
}
