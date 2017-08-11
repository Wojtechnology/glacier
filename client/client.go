package client

import (
	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/crypto"
)

type Client struct {
	url string
	me  *core.Node
}

func NewClient(url string, priv []byte) *Client {
	return &Client{url: url, me: core.NewNode(crypto.ParsePrivateKey(priv))}
}
