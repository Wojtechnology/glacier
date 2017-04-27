package merkle

import (
	"bytes"
	"fmt"
)

// Returns proof that a key exists within the trie
func BuildProof(t *MerkleTrie, target *MerkleLeafNode) ([]*MerkleBranchNode, error) {
	var (
		n      = t.root
		proof  = make([]*MerkleBranchNode, 0)
		hasher = NewHasher()
		found  = false
	)

	for !found {
		var err error
		n, err = t.maybeResolveNode(n)
		if err != nil {
			return nil, err
		}

		switch tn := n.(type) {
		case *MerkleLeafNode:
			if !target.Equals(tn) {
				// TODO: Prehaps only require key equality
				return nil, NotFoundError{Key: target.key}
			}
			found = true
		case *MerkleBranchNode:
			proof = append(proof, tn)
			if bytes.Equal(target.key, tn.keyPrefix) {
				n = tn.innerLeaf
			} else if len(longestCommonPrefix(target.key, tn.keyPrefix)) == tn.Len() {
				n = tn.child(target.key)
			} else {
				return nil, NotFoundError{Key: target.key}
			}
		case nil:
			return nil, NotFoundError{Key: target.key}
		default:
			panic(fmt.Sprintf("Invalid node type: %T, %s", tn, tn))
		}
	}

	for i, branch := range proof {
		proof[i] = hasher.hashChildren(branch)
	}

	return proof, nil
}

func VerifyProof(hash []byte, target *MerkleLeafNode, proof []*MerkleBranchNode) bool {
	hasher := NewHasher()

	for _, branch := range proof {
		if !bytes.Equal(hash, hasher.hash(branch)) {
			return false
		}

		var child MerkleNode
		if bytes.Equal(target.key, branch.keyPrefix) {
			child = branch.innerLeaf
		} else if len(longestCommonPrefix(target.key, branch.keyPrefix)) == branch.Len() {
			child = branch.child(target.key)
		} else {
			return false // The given proof path is invalid
		}

		hash = hasher.hash(child)
	}

	if !bytes.Equal(hash, hasher.hash(target)) {
		return false
	}
	return true
}
