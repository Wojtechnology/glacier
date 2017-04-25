package merkle

import (
	"fmt"
	"golang.org/x/crypto/sha3"
	"hash"

	"github.com/ethereum/go-ethereum/rlp"
)

type Hasher struct {
	hash hash.Hash
}

func NewHasher() *Hasher {
	h := &Hasher{hash: sha3.New256()}
	return h
}

type merkleLeafData struct {
	key []byte
	val interface{}
}

// Hashes the passed in node that exists in the given trie, returns the hash
func (h *Hasher) Hash(t *MerkleTrie, n MerkleNode) ([]byte, error) {
	var hash []byte

	switch tn := n.(type) {
	case *MerkleLeafNode:
		hash = tn.hash()
		if hash != nil {
			return hash, nil
		}

		hash := h.hashLeaf(tn)
		tn.setHash(hash)
		return hash, nil
	case *MerkleBranchNode:
		hash = tn.hash()
		if hash != nil {
			return hash, nil
		}

		childHashes := make([][]byte, 17)
		var err error
		for i, child := range tn.children {
			childHashes[i], err = h.Hash(t, child)
			if err != nil {
				return nil, err
			}
		}
		childHashes[16], err = h.Hash(t, tn.innerLeaf)
		if err != nil {
			return nil, err
		}

		hash := hashObject(childHashes, h.hash)
		tn.setHash(hash)
		return hash, nil
	case *MerkleHashNode:
		return tn.hash, nil
	case nil:
		return hashNil(), nil
	default:
		panic(fmt.Sprintf("Invalid node type: %T, %s", tn, tn))
	}
}

func (h *Hasher) hashLeaf(n *MerkleLeafNode) []byte {
	return hashObject(merkleLeafData{key: n.key, val: n.val}, h.hash)
}

func (h *Hasher) hashBranch(n *MerkleBranchNode) []byte {
	childHashes := make([][]byte, 17)
	for i, child := range n.children {
		childHashes[i] = h.hashNode(child)
	}
	childHashes[16] = h.hashNode(n.innerLeaf)
	return hashObject(childHashes, h.hash)
}

func (h *Hasher) hashNode(n MerkleNode) []byte {
	// TODO: write separate test
	switch tn := n.(type) {
	case *MerkleLeafNode:
		return h.hashLeaf(tn)
	case *MerkleBranchNode:
		return h.hashBranch(tn)
	case *MerkleHashNode:
		return tn.hash
	case nil:
		return hashNil()
	default:
		panic(fmt.Sprintf("Invalid node type: %T, %s", tn, tn))
	}
}

func hashNil() []byte {
	return []byte{78, 73, 76}
}

func hashObject(o interface{}, h hash.Hash) []byte {
	defer h.Reset()
	hash := make([]byte, 32)
	rlp.Encode(h, o)
	h.Sum(hash[:0])
	return hash
}
