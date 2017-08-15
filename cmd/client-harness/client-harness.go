// NOTE: This is a temporary executable and will quickly be replaced by glash
package main

import (
	"crypto/ecdsa"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/wojtechnology/glacier/client"
	"github.com/wojtechnology/glacier/crypto"
)

func privFromFile(path string) *ecdsa.PrivateKey {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Printf("Error when reading private key: %s\n", err.Error())
		os.Exit(1)
	}

	return crypto.ParsePrivateKey(data)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: client-harness <glacier_url> <priv_key_file>")
		os.Exit(1)
	}

	priv := privFromFile(os.Args[2])
	_ = crypto.MarshalPublicKey(&priv.PublicKey)
	c := client.NewClient(os.Args[1], priv)
	// err := c.CreateTable([]byte("wtf"), []map[string][]byte{
	// 	map[string][]byte{
	// 		"type":      []byte("table_exists"),
	// 		"TableName": []byte("wtf"),
	// 	},
	// 	map[string][]byte{
	// 		"type":      []byte("admin"),
	// 		"TableName": []byte("wtf"),
	// 		"PubKey":    pub,
	// 	},
	// })
	err := c.UpdateTable([]byte("wtf"),
		[]map[string][]byte{
			map[string][]byte{
				"type":      []byte("all_writers"),
				"TableName": []byte("wtf"),
			},
		}, client.InputFlag(0))

	if err != nil {
		panic(err)
	}
}
