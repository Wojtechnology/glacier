package core

import (
	"crypto/ecdsa"

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
