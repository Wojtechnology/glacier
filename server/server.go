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

	rowId := bytesYo(61)
	fmt.Printf("RowId: %v, len(%d)\n", rowId, len(rowId))
	colId := bytesYo(24)
	fmt.Printf("ColId: %v, len(%d)\n", colId, len(colId))
	op := meddb.NewPutOp(rowId)
	op.AddCol(colId, []byte("LIT"))

	err = db.Put([]byte("HELLO"), op)
	if err != nil {
		panic(err)
	}

	res, err := db.Get([]byte("HELLO"), meddb.NewGetOp(rowId, [][]byte{colId}))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", res)
}

func bytesYo(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = 255
	}
	return b
}
