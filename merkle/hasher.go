package merkle

import (
	"fmt"
	"golang.org/x/crypto/sha3"
	"hash"

	"github.com/ethereum/go-ethereum/rlp"
)

type Hasher struct {
	sha hash.Hash
}

func NewHasher() *Hasher {
	h := &Hasher{sha: sha3.New256()}
	return h
}

type MerkleLeafData struct {
	Key []byte
	Val interface{}
}

// Hashes the passed in node, and returns its hash
// Updates the caches of dirty nodes (only for nodes that are accessed during this operation)
func (h *Hasher) hash(n MerkleNode) []byte {
	var hash []byte

	switch tn := n.(type) {
	case *MerkleLeafNode:
		hash = tn.hash()
		if hash != nil {
			return hash
		}

		hash := h.hashLeaf(tn)
		tn.setHash(hash)
		return hash
	case *MerkleBranchNode:
		hash = tn.hash()
		if hash != nil {
			return hash
		}

		childHashes := make([][]byte, 17)
		for i, child := range tn.children {
			childHashes[i] = h.hash(child)
		}
		childHashes[16] = h.hash(tn.innerLeaf)

		hash := hashObject(childHashes, h.sha)
		tn.setHash(hash)
		return hash
	case *MerkleHashNode:
		return tn.hash
	case nil:
		return hashNil()
	default:
		panic(fmt.Sprintf("Invalid node type: %T, %s", tn, tn))
	}
}

func (h *Hasher) hashChildren(n *MerkleBranchNode) *MerkleBranchNode {
	branch := &MerkleBranchNode{keyPrefix: n.keyPrefix}

	for i, child := range n.children {
		branch.children[i] = &MerkleHashNode{hash: h.hash(child)}
	}
	branch.innerLeaf = &MerkleHashNode{hash: h.hash(n.innerLeaf)}

	return branch
}

func (h *Hasher) hashLeaf(n *MerkleLeafNode) []byte {
	// TODO: Prehaps only require key equality
	return hashObject(MerkleLeafData{Key: n.key, Val: n.val}, h.sha)
}

func hashNil() []byte {
	return []byte{}
}

func hashObject(o interface{}, h hash.Hash) []byte {
	defer h.Reset()
	hash := make([]byte, 32)
	rlp.Encode(h, o)
	h.Sum(hash[:0])
	return hash
}
