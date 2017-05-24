package main

import (
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
	print("Listening on " + PORT + "\n")
	http.ListenAndServe(":"+PORT, nil)
}

func main() {
	addresses := make([]string, 1)
	addresses[0] = "127.0.0.1"
	db, err := meddb.NewRethinkBigtable(addresses)
	if err != nil {
		panic(err)
	}

	err = db.CreateTable([]byte("HELLO"))
	if err != nil {
		panic(err)
	}

	serverInit()
}
