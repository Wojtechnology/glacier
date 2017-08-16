package handler

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/logging"
)

var blockchain *core.Blockchain

func SetBlockchain(bc *core.Blockchain) {
	blockchain = bc
}

func SetupRoutes() {
	http.HandleFunc("/transaction/", handleTransaction)
}

// --------------------
// JSON Data Structures
// --------------------

type CellData struct {
	Data  string   `json:"data"` // Base64 encoded
	VerId *big.Int `json:"ver_id"`
}

type OutputData struct {
	Type int    `json:"type"`
	Data string `json:"data"` // Base64 encoded
}

type InputData struct {
	Type       int    `json:"type"`
	OutputHash string `json:"output_hash"` // Base64 encoded
	Data       string `json:"data"`        // Base64 encoded
}

type TransactionData struct {
	Type      int                  `json:"type"`
	TableName string               `json:"table_name"`
	RowId     string               `json:"row_id"`
	Cols      map[string]*CellData `json:"cols"`
	Inputs    []*InputData         `json:"inputs"`
	Outputs   []*OutputData        `json:"outputs"`
}

// --------
// Handlers
// --------

func badRequest(w http.ResponseWriter, err error) {
	logging.Error(err.Error())
	w.WriteHeader(400)
	fmt.Fprintf(w, "bad request\n")
}

func handleTransaction(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/transaction/" {
		w.WriteHeader(404)
		fmt.Fprintf(w, "not found\n")
		return
	}

	switch r.Method {
	case "POST":
		var tr TransactionData
		defer r.Body.Close()
		if err := jsonDecode(r, &tr); err != nil {
			return
		}
		tx, err := tr.toCoreTransaction()
		if err != nil {
			badRequest(w, err)
			return
		}
		if err := blockchain.AddTransaction(tx); err != nil {
			badRequest(w, err)
			return
		}

	default:
		w.WriteHeader(404)
		fmt.Fprintf(w, "not found\n")
	}
}

// -------
// Helpers
// -------

func jsonDecode(r *http.Request, o interface{}) error {
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(o)
	if err != nil {
		return err
	}
	return nil
}

// ---------------------------
// JSON Data Structure Mappers
// ---------------------------

func (c *CellData) toCoreCell() (*core.Cell, error) {
	var verId *big.Int = nil
	if c.VerId != nil {
		verId = big.NewInt(c.VerId.Int64())
	}
	data, err := base64.StdEncoding.DecodeString(c.Data)
	if err != nil {
		return nil, err
	}
	return &core.Cell{
		Data:  data,
		VerId: verId,
	}, nil
}

func fromCoreCell(c *core.Cell) *CellData {
	var verId *big.Int = nil
	if c.VerId != nil {
		verId = big.NewInt(c.VerId.Int64())
	}
	return &CellData{
		Data:  base64.StdEncoding.EncodeToString(c.Data),
		VerId: verId,
	}
}

func (o *OutputData) toCoreOutput() (core.Output, error) {
	data, err := base64.StdEncoding.DecodeString(o.Data)
	if err != nil {
		return nil, err
	}
	output, err := core.NewOutput(core.OutputType(o.Type), data)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func fromCoreOutput(o core.Output) *OutputData {
	return &OutputData{
		Type: int(o.Type()),
		Data: base64.StdEncoding.EncodeToString(o.Data()),
	}
}

func (i *InputData) toCoreInput() (core.Input, error) {
	outputHash, err := base64.StdEncoding.DecodeString(i.OutputHash)
	if err != nil {
		return nil, err
	}
	data, err := base64.StdEncoding.DecodeString(i.Data)
	if err != nil {
		return nil, err
	}
	input, err := core.NewInput(core.InputType(i.Type), outputHash, data)
	if err != nil {
		return nil, err
	}
	return input, nil
}

func fromCoreInput(i core.Input) *InputData {
	return &InputData{
		Type:       int(i.Type()),
		OutputHash: base64.StdEncoding.EncodeToString(i.OutputHash().Bytes()),
		Data:       base64.StdEncoding.EncodeToString(i.Data()),
	}
}

func (tr *TransactionData) toCoreTransaction() (*core.Transaction, error) {
	var cols map[string]*core.Cell = nil
	if tr.Cols != nil {
		cols = make(map[string]*core.Cell)
		for colId, cell := range tr.Cols {
			var err error
			cols[colId], err = cell.toCoreCell()
			if err != nil {
				return nil, err
			}
		}
	}

	var outputs []core.Output = nil
	if tr.Outputs != nil {
		outputs = make([]core.Output, len(tr.Outputs))
		for i, output := range tr.Outputs {
			var err error
			outputs[i], err = output.toCoreOutput()
			if err != nil {
				return nil, err
			}
		}
	}

	var inputs []core.Input = nil
	if tr.Inputs != nil {
		inputs = make([]core.Input, len(tr.Inputs))
		for i, input := range tr.Inputs {
			var err error
			inputs[i], err = input.toCoreInput()
			if err != nil {
				return nil, err
			}
		}
	}

	tx := &core.Transaction{
		Type:      core.TransactionType(tr.Type),
		TableName: []byte(tr.TableName),
		RowId:     []byte(tr.RowId),
		Cols:      cols,
		Outputs:   outputs,
		Inputs:    inputs,
	}

	return tx, nil
}

func FromCoreTransaction(tx *core.Transaction) *TransactionData {
	var cols map[string]*CellData = nil
	if tx.Cols != nil {
		cols = make(map[string]*CellData)
		for colId, cell := range tx.Cols {
			cols[colId] = fromCoreCell(cell)
		}
	}

	var outputs []*OutputData = nil
	if tx.Outputs != nil {
		outputs = make([]*OutputData, len(tx.Outputs))
		for i, output := range tx.Outputs {
			outputs[i] = fromCoreOutput(output)
		}
	}

	var inputs []*InputData = nil
	if tx.Inputs != nil {
		inputs = make([]*InputData, len(tx.Inputs))
		for i, input := range tx.Inputs {
			inputs[i] = fromCoreInput(input)
		}
	}

	return &TransactionData{
		Type:      int(tx.Type),
		TableName: string(tx.TableName),
		RowId:     string(tx.RowId),
		Cols:      cols,
		Outputs:   outputs,
		Inputs:    inputs,
	}
}
