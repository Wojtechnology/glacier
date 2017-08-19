package core

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/wojtechnology/glacier/crypto"
)

type Rule interface {
	RequestedOutputIds(*Transaction) map[string]OutputRequirement
	Validate(*Transaction, map[string]Output, map[string][]Input) error
}

// Defines OutputRequirement "enum"
// Essentially specifies how strict we are about whether we could find the output when validating
// a transaction. Rules returns some set of outputs, and not all of them are required. In fact,
// sometimes we are looking for the lack of an output.
type OutputRequirement int

const (
	// If the output is missing, we ignore it.
	OUTPUT_REQUIREMENT_NONE OutputRequirement = iota // NONE = 0
	// NONE + if the output is undecided, we place it back in the backlog
	OUTPUT_REQUIREMENT_DECIDED // DECIDED = 1
	// DECIDED + if the output is missing or rejected, transaction is invalid
	OUTPUT_REQUIREMENT_REQUIRED // REQUIRED = 2
)

// --------------------------------
// TableExistsOutputMixin
// --------------------------------

type TableExistsOutputMixin struct{}

func (mixin *TableExistsOutputMixin) getTableExistsOutputHash(tx *Transaction) Hash {
	return HashOutput(&TableExistsOutput{&TableNameMixin{tx.TableName}})
}

func (mixin *TableExistsOutputMixin) RequestedOutputIds(
	tx *Transaction) map[string]OutputRequirement {

	return map[string]OutputRequirement{
		mixin.getTableExistsOutputHash(tx).String(): OUTPUT_REQUIREMENT_DECIDED,
	}
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
	return HashOutput(&AllColsAllowedOutput{&TableNameMixin{tx.TableName}})
}

func (rule *ColsAllowedRule) getColAllowedOutputHashes(tx *Transaction) []Hash {
	hashes := make([]Hash, len(tx.Cols))

	i := 0
	for colId, _ := range tx.Cols {
		hashes[i] = HashOutput(&ColAllowedOutput{
			TableNameMixin: &TableNameMixin{tx.TableName},
			ColName:        []byte(colId),
		})
		i++
	}

	return hashes
}

func (rule *ColsAllowedRule) RequestedOutputIds(tx *Transaction) map[string]OutputRequirement {
	colAllowedHashes := rule.getColAllowedOutputHashes(tx)
	outputReqs := map[string]OutputRequirement{}

	for _, hash := range colAllowedHashes {
		outputReqs[hash.String()] = OUTPUT_REQUIREMENT_NONE
	}

	outputReqs[rule.getAllColsAllowedOutputHash(tx).String()] = OUTPUT_REQUIREMENT_NONE
	return outputReqs
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

// --------------------------------
// AdminRule implementation
//
// Used to check whether given admin can update the table
// --------------------------------

type AdminRule struct{}

func (rule *AdminRule) getAllAdminsOutputHash(tx *Transaction) Hash {
	return HashOutput(&AllAdminsOutput{&TableNameMixin{tx.TableName}})
}

func (rule *AdminRule) RequestedOutputIds(tx *Transaction) map[string]OutputRequirement {
	return map[string]OutputRequirement{
		rule.getAllAdminsOutputHash(tx).String(): OUTPUT_REQUIREMENT_NONE,
	}
}

func (rule *AdminRule) Validate(tx *Transaction, linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	allAdminsHash := rule.getAllAdminsOutputHash(tx).String()
	if _, ok := linkedOutputs[allAdminsHash]; ok {
		// All admins can update the table
		return nil
	}

	adminInputs := make([]*AdminInput, 0)
	for _, input := range tx.Inputs {
		if adminInput, ok := input.(*AdminInput); ok {
			adminInputs = append(adminInputs, adminInput)
		}
	}

	if len(adminInputs) != 1 {
		return errors.New(fmt.Sprintf("Must have exactly 1 admin input. Have %v\n",
			len(adminInputs)))
	}

	adminInput := adminInputs[0]
	output, outputExists := linkedOutputs[adminInput.OutputHash().String()]
	if !outputExists {
		return errors.New(fmt.Sprintf("Output missing for admin rule: %v\n",
			adminInput.OutputHash().Bytes()))
	}

	adminOutput, outputTypeCorrect := output.(*AdminOutput)
	if !outputTypeCorrect {
		return errors.New(fmt.Sprintf("Invalid output type for admin rule: %v\n", output))
	}

	pubKey, err := crypto.RetrievePublicKey(tx.Hash().Bytes(), adminInput.Sig)
	if err != nil {
		return err
	}

	if !bytes.Equal(pubKey, adminOutput.PubKey) {
		return errors.New("Signature invalid\n")
	}

	return nil
}

// --------------------------------
// WriterRule implementation
//
// Used to check whether user can write to the table
// --------------------------------

type WriterRule struct{}

func (rule *WriterRule) getAllWritersOutputHash(tx *Transaction) Hash {
	return HashOutput(&AllWritersOutput{&TableNameMixin{tx.TableName}})
}

func (rule *WriterRule) RequestedOutputIds(tx *Transaction) map[string]OutputRequirement {
	return map[string]OutputRequirement{
		rule.getAllWritersOutputHash(tx).String(): OUTPUT_REQUIREMENT_NONE,
	}
}

func (rule *WriterRule) Validate(tx *Transaction, linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	allWritersHash := rule.getAllWritersOutputHash(tx).String()
	if _, ok := linkedOutputs[allWritersHash]; ok {
		// All writers can update the table
		return nil
	}

	writerInputs := make([]*WriterInput, 0)
	for _, input := range tx.Inputs {
		if writerInput, ok := input.(*WriterInput); ok {
			writerInputs = append(writerInputs, writerInput)
		}
	}

	if len(writerInputs) != 1 {
		return errors.New(fmt.Sprintf("Must have exactly 1 writer input. Have %v\n",
			len(writerInputs)))
	}

	writerInput := writerInputs[0]
	output, outputExists := linkedOutputs[writerInput.OutputHash().String()]
	if !outputExists {
		return errors.New(fmt.Sprintf("Output missing for writer rule: %v\n",
			writerInput.OutputHash().Bytes()))
	}

	writerOutput, outputTypeCorrect := output.(*WriterOutput)
	if !outputTypeCorrect {
		return errors.New(fmt.Sprintf("Invalid output type for writer rule: %v\n", output))
	}

	pubKey, err := crypto.RetrievePublicKey(tx.Hash().Bytes(), writerInput.Sig)
	if err != nil {
		return err
	}

	if !bytes.Equal(pubKey, writerOutput.PubKey) {
		return errors.New("Signature invalid\n")
	}

	return nil
}

// --------------------------------
// RowRule implementation
//
// Used to check whether user can write to the given row
// --------------------------------

type RowRule struct{}

func (rule *RowRule) getAllRowWritersOutputHash(tx *Transaction) Hash {
	return HashOutput(&AllRowWritersOutput{
		TableNameMixin: &TableNameMixin{tx.TableName},
		RowId:          tx.RowId,
	})
}

func (rule *RowRule) RequestedOutputIds(tx *Transaction) map[string]OutputRequirement {
	return map[string]OutputRequirement{
		rule.getAllRowWritersOutputHash(tx).String(): OUTPUT_REQUIREMENT_NONE,
	}
}

func (rule *RowRule) Validate(tx *Transaction, linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	allRowWritersHash := rule.getAllRowWritersOutputHash(tx).String()
	if _, ok := linkedOutputs[allRowWritersHash]; ok {
		// All writers can write to the row
		return nil
	}

	rowWriterInputs := make([]*RowWriterInput, 0)
	for _, input := range tx.Inputs {
		if rowWriterInput, ok := input.(*RowWriterInput); ok {
			rowWriterInputs = append(rowWriterInputs, rowWriterInput)
		}
	}

	if len(rowWriterInputs) != 1 {
		return errors.New(fmt.Sprintf("Must have exactly 1 row writer input. Have %v\n",
			len(rowWriterInputs)))
	}

	rowWriterInput := rowWriterInputs[0]
	output, outputExists := linkedOutputs[rowWriterInput.OutputHash().String()]
	if !outputExists {
		return errors.New(fmt.Sprintf("Output missing for row writer rule: %v\n",
			rowWriterInput.OutputHash().Bytes()))
	}

	rowWriterOutput, outputTypeCorrect := output.(*RowWriterOutput)
	if !outputTypeCorrect {
		return errors.New(fmt.Sprintf("Invalid output type for row writer rule: %v\n", output))
	}

	pubKey, err := crypto.RetrievePublicKey(tx.Hash().Bytes(), rowWriterInput.Sig)
	if err != nil {
		return err
	}

	if !bytes.Equal(pubKey, rowWriterOutput.PubKey) {
		return errors.New("Signature invalid\n")
	}

	return nil
}

// --------------------------------
// ValidOutputTypesRule implementation
//
// Used to check whether transaction only has certain output types
// --------------------------------

type ValidOutputTypesRule struct {
	validTypes map[OutputType]bool // map is used as a set here
}

func (rule *ValidOutputTypesRule) RequestedOutputIds(tx *Transaction) map[string]OutputRequirement {
	return map[string]OutputRequirement{}
}

func (rule *ValidOutputTypesRule) Validate(tx *Transaction, linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	for _, output := range tx.Outputs {
		if _, ok := rule.validTypes[output.Type()]; !ok {
			return errors.New(fmt.Sprintf("Invalid output type: %d\nExpected: %v\n",
				output.Type(), rule.validTypes))
		}
	}

	return nil
}

// --------------------------------
// HasTableExistsRule implementation
//
// Used to check whether a transaction has a TABLE_EXISTS output
// --------------------------------

type HasTableExistsRule struct{}

func (rule *HasTableExistsRule) RequestedOutputIds(tx *Transaction) map[string]OutputRequirement {
	return map[string]OutputRequirement{}
}

func (rule *HasTableExistsRule) Validate(tx *Transaction, linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	for _, output := range tx.Outputs {
		if output.Type() == OUTPUT_TYPE_TABLE_EXISTS {
			return nil
		}
	}

	return errors.New(fmt.Sprintf("Transaction doesn't have a TABLE_EXISTS output type"))
}
