package client

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/crypto"
	"github.com/wojtechnology/glacier/handler"
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

func NewClient(url string, priv *ecdsa.PrivateKey) *Client {
	return &Client{url: url, me: core.NewNode(priv)}
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
	err = c.postTransaction(tx)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) UpdateTable(tableName []byte, outputs []map[string][]byte,
	inputFlag InputFlag) error {

	coreOutputs, err := outputsFromMaps(outputs)
	if err != nil {
		return err
	}
	tx := &core.Transaction{
		Type:      core.TRANSACTION_TYPE_UPDATE_TABLE,
		TableName: tableName,
		Outputs:   coreOutputs,
	}
	err = c.populateAndSignInputs(tx, inputFlag)
	if err != nil {
		return err
	}
	err = c.postTransaction(tx)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) PutCells(tableName, rowId []byte, cols map[string]*core.Cell,
	outputs []map[string][]byte, inputFlag InputFlag) error {

	coreOutputs, err := outputsFromMaps(outputs)
	if err != nil {
		return err
	}
	tx := &core.Transaction{
		Type:      core.TRANSACTION_TYPE_PUT_CELLS,
		TableName: tableName,
		RowId:     rowId,
		Cols:      cols,
		Outputs:   coreOutputs,
	}
	err = c.populateAndSignInputs(tx, inputFlag)
	if err != nil {
		return err
	}
	err = c.postTransaction(tx)
	if err != nil {
		return err
	}

	return nil
}

// Populates the transaction with signed inputs according the the given `inputFlag`.
// This happens in two steps:
// 1) Populate transaction with inputs that are missing signatures and get the transaction hash.
// Note that the transaction hash contains the hashes of all of the inputs, but the hashes of the
// inputs do not contain signature (since that would create a circular dependency).
// 2) Sign the transaction hash using my private key and populate the signatures for all of the
// inputs.
func (c *Client) populateAndSignInputs(tx *core.Transaction, inputFlag InputFlag) error {
	coreInputs := make([]core.Input, 0)
	if inputFlag&INPUT_FLAG_ADMIN != 0 {
		assocOutput := &core.AdminOutput{
			TableNameMixin: core.TableNameMixin{Table: tx.TableName},
			PubKey:         c.me.PubKey,
		}
		coreInput := &core.AdminInput{InputLink: core.InputLink{
			LinksTo: core.HashOutput(assocOutput)},
		}
		coreInputs = append(coreInputs, coreInput)
	}
	if inputFlag&INPUT_FLAG_WRITER != 0 {
		assocOutput := &core.WriterOutput{
			TableNameMixin: core.TableNameMixin{Table: tx.TableName},
			PubKey:         c.me.PubKey,
		}
		coreInput := &core.WriterInput{InputLink: core.InputLink{
			LinksTo: core.HashOutput(assocOutput)},
		}
		coreInputs = append(coreInputs, coreInput)
	}
	if inputFlag&INPUT_FLAG_ROW_WRITER != 0 {
		assocOutput := &core.RowWriterOutput{
			TableNameMixin: core.TableNameMixin{Table: tx.TableName},
			PubKey:         c.me.PubKey,
		}
		coreInput := &core.RowWriterInput{InputLink: core.InputLink{
			LinksTo: core.HashOutput(assocOutput)},
		}
		coreInputs = append(coreInputs, coreInput)
	}

	tx.Inputs = coreInputs
	sig, err := crypto.Sign(tx.Hash().Bytes(), c.me.PrivKey)
	if err != nil {
		return err
	}

	for _, coreInput := range tx.Inputs {
		err := coreInput.FromData(sig)
		if err != nil {
			return err
		}
	}

	return nil
}

// Actually makes request to server using the given `url` in the client.
// Doesn't modify the transaction.
func (c *Client) postTransaction(tx *core.Transaction) error {
	td := handler.FromCoreTransaction(tx)
	data := new(bytes.Buffer)
	err := json.NewEncoder(data).Encode(td)

	res, err := http.Post(c.url+"/transaction/", "application/json; charset=utf-8", data)
	if err != nil {
		return err
	}

	fmt.Printf("%v", res.Status)

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
