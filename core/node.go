package core

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/wojtechnology/glacier/crypto"
)

type Node struct {
	PubKey  []byte
	PrivKey *ecdsa.PrivateKey
}

func NewNode(priv *ecdsa.PrivateKey) *Node {
	return &Node{
		PubKey:  crypto.MarshalPublicKey(&priv.PublicKey),
		PrivKey: priv,
	}
}

func NewNodeFromFile(path string) (*Node, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error when reading private key: %s\n", err.Error()))
	}

	return NewNode(crypto.ParsePrivateKey(data)), nil
}
