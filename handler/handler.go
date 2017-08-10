package handler

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	"github.com/wojtechnology/glacier/core"
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

type Cell struct {
	Data  string   `json:"data"`
	VerId *big.Int `json:"ver_id"`
}

type Output struct {
	Type int    `json:"type"`
	Data string `json:"data"`
}

type Input struct {
	Type       int    `json:"type"`
	OutputHash string `json:"output_hash"`
	Data       string `json:"data"`
}

type TransactionRequest struct {
	TableName string                      `json:"table_name"`
	RowId     string                      `json:"row_id"`
	Cols      map[string]*json.RawMessage `json:"cols"`
	Inputs    []*json.RawMessage          `json:"inputs"`
	Outputs   []*json.RawMessage          `json:"outputs"`
}

func (c *Cell) toCoreCell() *core.Cell {
	var verId *big.Int = nil
	if c.VerId != nil {
		verId = big.NewInt(c.VerId.Int64())
	}
	return &core.Cell{
		Data:  []byte(c.Data),
		VerId: verId,
	}
}

func (tr *TransactionRequest) toCoreTransaction() (*core.Transaction, error) {
	var cols map[string]*core.Cell = nil
	if tr.Cols != nil {
		cols = make(map[string]*core.Cell)
		for colId, rawCell := range tr.Cols {
			var c *Cell
			if err := json.Unmarshal(*rawCell, &c); err != nil {
				return nil, err
			}
			cols[colId] = c.toCoreCell()
		}
	}

	return &core.Transaction{
		TableName: []byte(tr.TableName),
		RowId:     []byte(tr.RowId),
		Cols:      cols,
	}, nil
}

func handleTransaction(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/transaction/" {
		w.WriteHeader(404)
		fmt.Fprintf(w, "not found\n")
		return
	}

	switch r.Method {
	case "POST":
		var tr TransactionRequest
		defer r.Body.Close()
		if err := jsonDecode(r, &tr); err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, "bad request\n")
			return
		}
		tx, err := tr.toCoreTransaction()
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, "bad request\n")
			return
		}
		if err := blockchain.AddTransaction(tx); err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, "bad request\n")
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
