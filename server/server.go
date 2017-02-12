package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/wojtechnology/medblocks/crypto"
	"github.com/wojtechnology/medblocks/ledger"
)

const PORT = "8000"

func ecdsaExample() {
	priv, err := crypto.CreateKey()
	if err != nil {
		panic(err)
	}
	hash := []byte("YOLO FAM")
	r, s, err := crypto.Sign(hash, priv)
	// should return true
	fmt.Println(crypto.Verify(hash, &priv.PublicKey, r, s))
}

type NewUserResponse struct {
	Uid        string `json:"uid"`
	PrivateKey string `json:"private_key"`
}

func newUser(w http.ResponseWriter, r *http.Request) {
	priv, err := crypto.CreateKey()
	if err != nil {
		io.WriteString(w, "Error creating key\n")
	}

	var user *ledger.User
	user, err = ledger.WriteUser(&priv.PublicKey)
	if err != nil {
		io.WriteString(w, "Unable to create user\n")
	}

	var privEncoded []byte
	privEncoded, err = crypto.SerializePrivateKey(priv)
	if err != nil {
		io.WriteString(w, "Unable to serialize private key\n")
	}

	var resString []byte
	response := NewUserResponse{Uid: user.Id, PrivateKey: string(privEncoded)}
	resString, err = json.Marshal(response)
	if err != nil {
		io.WriteString(w, "Error serializing response\n")
	}

	io.WriteString(w, string(resString))
}

func setRoutes() {
	http.HandleFunc("/new_user", newUser)
}

func serverInit() {
	err := ledger.InitDB()
	if err != nil {
		panic(err)
	}
	defer ledger.Session.Close()

	setRoutes()
	print("Listening on " + PORT + "\n")
	http.ListenAndServe(":"+PORT, nil)
}

func main() {
	serverInit()
}
