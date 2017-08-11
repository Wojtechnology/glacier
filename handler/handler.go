package handler

import (
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

// --------
// Handlers
// --------

type CellData struct {
	Data  string   `json:"data"`
	VerId *big.Int `json:"ver_id"`
}

type OutputData struct {
	Type int    `json:"type"`
	Data string `json:"data"`
}

type InputData struct {
	Type       int    `json:"type"`
	OutputHash string `json:"output_hash"`
	Data       string `json:"data"`
}

type TransactionData struct {
	Type      int                         `json:"type"`
	TableName string                      `json:"table_name"`
	RowId     string                      `json:"row_id"`
	Cols      map[string]*json.RawMessage `json:"cols"`
	Inputs    []*json.RawMessage          `json:"inputs"`
	Outputs   []*json.RawMessage          `json:"outputs"`
}

func (c *CellData) toCoreCell() *core.Cell {
	var verId *big.Int = nil
	if c.VerId != nil {
		verId = big.NewInt(c.VerId.Int64())
	}
	return &core.Cell{
		Data:  []byte(c.Data),
		VerId: verId,
	}
}

func (o *OutputData) toCoreOutput() (core.Output, error) {
	output, err := core.NewOutput(core.OutputType(o.Type), []byte(o.Data))
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (i *InputData) toCoreInput() (core.Input, error) {
	input, err := core.NewInput(core.InputType(i.Type), []byte(i.OutputHash), []byte(i.Data))
	if err != nil {
		return nil, err
	}
	return input, nil
}

func (tr *TransactionData) toCoreTransaction() (*core.Transaction, error) {
	var cols map[string]*core.Cell = nil
	if tr.Cols != nil {
		cols = make(map[string]*core.Cell)
		for colId, rawCell := range tr.Cols {
			var c *CellData
			if err := json.Unmarshal(*rawCell, &c); err != nil {
				return nil, err
			}
			cols[colId] = c.toCoreCell()
		}
	}

	var outputs []core.Output = nil
	if tr.Outputs != nil {
		outputs = make([]core.Output, len(tr.Outputs))
		for i, rawOutput := range tr.Outputs {
			var o *OutputData
			err := json.Unmarshal(*rawOutput, &o)
			if err != nil {
				return nil, err
			}
			outputs[i], err = o.toCoreOutput()
			if err != nil {
				return nil, err
			}
		}
	}

	var inputs []core.Input = nil
	if tr.Inputs != nil {
		inputs = make([]core.Input, len(tr.Inputs))
		for i, rawInput := range tr.Inputs {
			var o *InputData
			err := json.Unmarshal(*rawInput, &o)
			if err != nil {
				return nil, err
			}
			inputs[i], err = o.toCoreInput()
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

	logging.Info(fmt.Sprintf("Got transaction of type %d\n", tx.Type))
	return tx, nil
}

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
