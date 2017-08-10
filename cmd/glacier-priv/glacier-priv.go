package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/wojtechnology/glacier/crypto"
)

func printError(action string, err error) {
	fmt.Printf("Error when %s: %s\n", action, err.Error())
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: glacier-priv <priv_key_file>")
		os.Exit(1)
	}

	path := os.Args[1]
	priv, err := crypto.NewPrivateKey()
	if err != nil {
		printError("creating private key", err)
	}

	if err := ioutil.WriteFile(path, crypto.MarshalPrivateKey(priv), 0644); err != nil {
		printError("writing private key", err)
	}
}
