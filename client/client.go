package client

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/crypto"
)

type Client struct {
	url string
	me  *core.Node
}

// Tells the client which inputs to populate for the transaction before sending the request.
// Uses the client private key to sign the inputs.
// For example, passing in `INPUT_FLAG_ADMIN` will populate the transaction with an `AdminInput`
// which contains the signature using the client private key.
type InputFlag int

const (
	INPUT_FLAG_ADMIN InputFlag = 1 << iota
	INPUT_FLAG_WRITER
	INPUT_FLAG_ROW_WRITER
)

type Cell struct {
	data  []byte
	verId *big.Int
}

func NewCell(data []byte) *Cell {
	return NewCellVerId(data, nil)
}

func NewCellVerId(data []byte, verId *big.Int) *Cell {
	return &Cell{data: data, verId: verId}
}

func NewClient(url string, priv []byte) *Client {
	return &Client{url: url, me: core.NewNode(crypto.ParsePrivateKey(priv))}
}

func (c *Client) CreateTable(tableName []byte, outputs []map[string][]byte) error {
	coreOutputs, err := outputsFromMaps(outputs)
	if err != nil {
		return err
	}
	tx := &core.Transaction{
		Type:      core.TRANSACTION_TYPE_CREATE_TABLE,
		TableName: tableName,
		Outputs:   coreOutputs,
	}
	err = postTransaction(tx)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) UpdateTable(tableName []byte, outputs []map[string][]byte,
	inputFlag InputFlag) error {

	// TODO: Implement
	return nil
}

func (c *Client) PutCells(tableName, rowId []byte, cols map[string]*Cell,
	outputs []map[string][]byte, inputFlag InputFlag) error {

	// TODO: Implement
	return nil
}

// -------
// Helpers
// -------

var OUTPUT_NAME_MAP = map[string]core.OutputType{
	"table_exists":     core.OUTPUT_TYPE_TABLE_EXISTS,
	"col_allowed":      core.OUTPUT_TYPE_COL_ALLOWED,
	"all_cols_allowed": core.OUTPUT_TYPE_ALL_COLS_ALLOWED,
	"all_admins":       core.OUTPUT_TYPE_ALL_ADMINS,
	"admin":            core.OUTPUT_TYPE_ADMIN,
	"all_writers":      core.OUTPUT_TYPE_ALL_WRITERS,
	"writer":           core.OUTPUT_TYPE_WRITER,
	"all_row_writers":  core.OUTPUT_TYPE_ALL_ROW_WRITERS,
	"row_writer":       core.OUTPUT_TYPE_ROW_WRITER,
}

// Takes a list of maps that describe outputs and creates `core.Output` implementation objects
// that correspond to the "type" field in the map.
// Each map in the list must have a "type" field with the value as a string with the name of the
// output (take a look at `OUTPUT_NAME_MAP` above).
// Also, each map must contain the data required for each field in snakecase (as opposed to
// camelcase as in the struct definition). For example, in the case of `core.TableExistsOutput` the
// map might look like this: {"table_name": []byte("cars"), "type": "table_exists"}.
func outputsFromMaps(outputs []map[string][]byte) ([]core.Output, error) {
	coreOutputs := make([]core.Output, len(outputs))
	for i, output := range outputs {
		var outputType core.OutputType
		if tpe, ok := output["type"]; ok {
			if outputType, ok = OUTPUT_NAME_MAP[string(tpe)]; !ok {
				return nil, errors.New(fmt.Sprintf(
					"Invalid output type %s for output %d", string(tpe), i))
			}
		} else {
			return nil, errors.New(fmt.Sprintf("Output %d missing type field", i))
		}
		data := make(map[string][]byte)
		for fieldName, fieldValue := range output {
			if fieldName != "type" {
				// TODO: snake_case to capitalized camelCase
				data[fieldName] = fieldValue
			}
		}
		var err error
		coreOutputs[i], err = core.NewOutputFromMap(outputType, data)
		if err != nil {
			return nil, err
		}
	}
	return coreOutputs, nil
}

// Actually makes request to server using the given `url` in the client.
// Doesn't modify the transaction.
func postTransaction(tx *core.Transaction) error {
	// TODO: Implement
	return nil
}
