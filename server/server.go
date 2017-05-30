package main

import (
	"fmt"
	"io"
	"net/http"

	"github.com/wojtechnology/glacier/meddb"
)

const PORT = "8000"

func index(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "web3.0")
}

func setRoutes() {
	http.HandleFunc("/", index)
}

func serverInit() {
	setRoutes()
	fmt.Printf("Listening on %s\n", PORT)
	http.ListenAndServe(":"+PORT, nil)
}

func createTable(db meddb.Bigtable, tableName []byte) {
	if err := db.CreateTable(tableName); err != nil {
		panic(err)
	}
}

func main() {
	db, err := meddb.NewRethinkBigtable([]string{"127.0.0.1"}, "test")
	if err != nil {
		panic(err)
	}

	// createTable(db, []byte("HELLO"))

	rowId := []byte("AAAYYYYY")
	colId := []byte("HEYYOOO")
	putOp := meddb.NewPutOp(rowId)
	putOp.AddCol(colId, []byte("LIT"))

	err = db.Put([]byte("HELLO"), putOp)
	if err != nil {
		panic(err)
	}

	res, err := db.Get([]byte("HELLO"), meddb.NewGetOpLimit(rowId, [][]byte{colId, []byte("HEYYOO")}, 3))
	if err != nil {
		panic(err)
	}
	for col, cells := range res {
		fmt.Printf("ROW    %s:\n", string(cells[0].RowId))
		fmt.Printf("COLUMN %s:\n", string(col))
		for i, cell := range cells {
			fmt.Printf("\tRESULT %d:\n", i)
			fmt.Printf("\t\tVERSION: %d\n", cell.VerId.Int64())
			fmt.Printf("\t\tDATA:    %s\n", string(cell.Data))
		}
	}
}

func bytesYo(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = 255
	}
	return b
}
