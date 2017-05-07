package main

import (
	"io"
	"net/http"
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
	serverInit()
}
