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

type cellAddress struct {
	TableName string   `json:"table_name"`
	RowId     string   `json:"row_id"`
	ColId     string   `json:"col_id"`
	VerId     *big.Int `json:"ver_id"`
}

func (ca *cellAddress) toCoreCellAddress() *core.CellAddress {
	var verId *big.Int = nil
	if ca.VerId != nil {
		verId = big.NewInt(ca.VerId.Int64())
	}
	return &core.CellAddress{
		TableName: []byte(ca.TableName),
		RowId:     []byte(ca.RowId),
		ColId:     []byte(ca.ColId),
		VerId:     verId,
	}
}

type transactionRequest struct {
	CellAddress *cellAddress `json:"cell_address"`
	Data        string       `json:"data"`
}

func (tr *transactionRequest) toCoreTransaction() *core.Transaction {
	return &core.Transaction{
		CellAddress: tr.CellAddress.toCoreCellAddress(),
		Data:        []byte(tr.Data),
	}
}

func handleTransaction(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/transaction/" {
		w.WriteHeader(404)
		fmt.Fprintf(w, "not found\n")
		return
	}

	switch r.Method {
	case "POST":
		var tr transactionRequest
		defer r.Body.Close()
		if err := jsonDecode(r, &tr); err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, "bad request\n")
			return
		}

		if err := blockchain.AddTransaction(tr.toCoreTransaction()); err != nil {
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
