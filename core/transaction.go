package core

import (
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/wojtechnology/glacier/meddb"
)

type TransactionType int

const (
	TRANSACTION_TYPE_CREATE_TABLE TransactionType = iota // CREATE_TABLE = 0
	TRANSACTION_TYPE_UPDATE_TABLE                        // UPDATE_TABLE = 1
	TRANSACTION_TYPE_PUT_CELLS                           // PUT_CELLS = 2
)

type Cell struct {
	Data  []byte
	VerId *big.Int
}

type Transaction struct {
	AssignedTo []byte // TODO: Make more strict type for public keys
	AssignedAt *big.Int
	Type       TransactionType
	TableName  []byte
	RowId      []byte
	Cols       map[string]*Cell
	Outputs    []Output
	Inputs     []Input
}

var rulesets = map[TransactionType][]Rule{
	TRANSACTION_TYPE_CREATE_TABLE: []Rule{
		&TableMissingRule{},
		&ValidOutputTypesRule{validTypes: map[OutputType]bool{
			OUTPUT_TYPE_TABLE_EXISTS:     true,
			OUTPUT_TYPE_COL_ALLOWED:      true,
			OUTPUT_TYPE_ALL_COLS_ALLOWED: true,
			OUTPUT_TYPE_ALL_ADMINS:       true,
			OUTPUT_TYPE_ADMIN:            true,
			OUTPUT_TYPE_ALL_WRITERS:      true,
			OUTPUT_TYPE_WRITER:           true,
		}},
		&HasTableExistsRule{},
	},
	TRANSACTION_TYPE_UPDATE_TABLE: []Rule{
		&TableExistsRule{},
		&AdminRule{},
		&ValidOutputTypesRule{validTypes: map[OutputType]bool{
			OUTPUT_TYPE_COL_ALLOWED:      true,
			OUTPUT_TYPE_ALL_COLS_ALLOWED: true,
			OUTPUT_TYPE_ALL_ADMINS:       true,
			OUTPUT_TYPE_ADMIN:            true,
			OUTPUT_TYPE_ALL_WRITERS:      true,
			OUTPUT_TYPE_WRITER:           true,
		}},
	},
	TRANSACTION_TYPE_PUT_CELLS: []Rule{
		&TableExistsRule{},
		&ColsAllowedRule{},
		&WriterRule{},
		&RowRule{},
		&ValidOutputTypesRule{validTypes: map[OutputType]bool{
			OUTPUT_TYPE_ALL_ROW_WRITERS: true,
			OUTPUT_TYPE_ROW_WRITER:      true,
		}},
	},
}

// ---------------
// Transaction API
// ---------------

type colCell struct {
	ColId []byte
	Cell  *Cell
}

// Part of transaction used in hash
type transactionBody struct {
	Type         *big.Int
	TableName    []byte
	RowId        []byte
	Cols         []*colCell
	OutputHashes [][]byte
	InputHashes  [][]byte
}

func (tx *Transaction) Hash() Hash {
	var (
		cols         []*colCell = nil
		outputHashes [][]byte   = nil
		inputHashes  [][]byte   = nil
	)

	if tx.Cols != nil {
		cols = make([]*colCell, len(tx.Cols))
		i := 0
		for colId, cell := range tx.Cols {
			cols[i] = &colCell{ColId: []byte(colId), Cell: cell}
			i++
		}

		// Sorting makes the hash deterministic
		sort.Slice(cols, func(i, j int) bool {
			return string(cols[i].ColId) < string(cols[j].ColId)
		})
	}

	if tx.Outputs != nil {
		outputHashes = make([][]byte, len(tx.Outputs))
		for i, output := range tx.Outputs {
			outputHashes[i] = HashOutput(output).Bytes()
		}
	}

	if tx.Inputs != nil {
		inputHashes = make([][]byte, len(tx.Inputs))
		for i, input := range tx.Inputs {
			inputHashes[i] = HashInput(input).Bytes()
		}
	}

	return rlpHash(&transactionBody{
		Type:         intToBigInt(int(tx.Type)),
		TableName:    tx.TableName,
		RowId:        tx.RowId,
		Cols:         cols,
		OutputHashes: outputHashes,
		InputHashes:  inputHashes,
	})
}

func (tx *Transaction) GetRuleset() ([]Rule, error) {
	if ruleset, ok := rulesets[tx.Type]; ok {
		return ruleset, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Invalid tx type: %v\n", tx.Type))
	}
}

func (tx *Transaction) Validate(linkedOutputs map[string]Output,
	spentInputs map[string][]Input) error {

	ruleset, err := tx.GetRuleset()
	if err != nil {
		return err
	}

	errs := make([]error, 0)
	for _, rule := range ruleset {
		if err := rule.Validate(tx, linkedOutputs, spentInputs); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return &RuleErrors{Errors: errs}
	}
	return nil
}

func (tx *Transaction) toDBTransaction() *meddb.Transaction {
	var (
		lastAssigned *big.Int               = nil
		cols         map[string]*meddb.Cell = nil
		outputs      []*meddb.Output        = nil
		inputs       []*meddb.Input         = nil
	)

	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}

	if tx.Cols != nil {
		cols = make(map[string]*meddb.Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = toDBCell(cell)
		}
	}

	if tx.Outputs != nil {
		outputs = make([]*meddb.Output, len(tx.Outputs))
		for i, output := range tx.Outputs {
			outputs[i] = toDBOutput(output)
		}
	}

	if tx.Inputs != nil {
		inputs = make([]*meddb.Input, len(tx.Inputs))
		for i, input := range tx.Inputs {
			inputs[i] = toDBInput(input)
		}
	}

	// TODO(wojtek): Maybe make copies here
	return &meddb.Transaction{
		Hash:       tx.Hash().Bytes(),
		AssignedTo: tx.AssignedTo,
		AssignedAt: lastAssigned,
		Type:       int(tx.Type),
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
		Outputs:    outputs,
		Inputs:     inputs,
	}
}

func fromDBTransaction(tx *meddb.Transaction) *Transaction {
	var (
		lastAssigned *big.Int         = nil
		cols         map[string]*Cell = nil
		outputs      []Output         = nil
		inputs       []Input          = nil
	)

	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}

	if tx.Cols != nil {
		cols = make(map[string]*Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = fromDBCell(cell)
		}
	}

	if tx.Outputs != nil {
		outputs = make([]Output, len(tx.Outputs))
		for i, output := range tx.Outputs {
			// TODO: Log when error occurs, since this should not be able to error
			outputs[i], _ = NewOutput(OutputType(output.Type), output.Data)
		}
	}

	if tx.Inputs != nil {
		inputs = make([]Input, len(tx.Inputs))
		for i, input := range tx.Inputs {
			// TODO: Log when error occurs, since this should not be able to error
			inputs[i], _ = NewInput(InputType(input.Type), input.OutputHash, input.Data)
		}
	}

	// TODO(wojtek): Maybe make copies here
	return &Transaction{
		AssignedTo: tx.AssignedTo,
		AssignedAt: lastAssigned,
		Type:       TransactionType(tx.Type),
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
		Outputs:    outputs,
		Inputs:     inputs,
	}
}

func fromDBTransactions(dbTxs []*meddb.Transaction) []*Transaction {
	txs := make([]*Transaction, len(dbTxs))
	for i, dbTx := range dbTxs {
		txs[i] = fromDBTransaction(dbTx)
	}
	return txs
}

// --------
// Cell API
// --------

func toDBCell(cell *Cell) *meddb.Cell {
	var verId *big.Int = nil
	if cell.VerId != nil {
		verId = big.NewInt(cell.VerId.Int64())
	}
	return &meddb.Cell{
		Data:  cell.Data,
		VerId: verId,
	}
}

func fromDBCell(cell *meddb.Cell) *Cell {
	var verId *big.Int = nil
	if cell.VerId != nil {
		verId = big.NewInt(cell.VerId.Int64())
	}
	return &Cell{
		Data:  cell.Data,
		VerId: verId,
	}
}
