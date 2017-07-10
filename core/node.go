package core

import "crypto/ecdsa"

type Node struct {
	PubKey  []byte
	PrivKey *ecdsa.PrivateKey
}
