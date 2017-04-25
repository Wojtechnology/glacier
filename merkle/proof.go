package merkle

import (
	"bytes"
	"fmt"
)

// Returns proof that a key exists within the trie
func BuildProof(t *MerkleTrie, target *MerkleLeafNode) MerkleNode {
	return buildProofInner(t, t.root, target, NewHasher())
}

func buildProofInner(t *MerkleTrie, n MerkleNode, target *MerkleLeafNode,
	hasher *Hasher) MerkleNode {
	n, err := t.maybeResolveNode(n)
	if err != nil {
		// TODO: Log
		return nil
	}

	switch tn := n.(type) {
	case *MerkleLeafNode:
		if !target.Equals(tn) {
			// target does not exist in trie
			return nil
		}
		hash, err := hasher.Hash(t, tn)
		if err != nil {
			// TODO: Log
			return nil
		}
		return &MerkleHashNode{hash: hash}
	case *MerkleBranchNode:
		branch := &MerkleBranchNode{
			keyPrefix: tn.keyPrefix,
		}
		if bytes.Equal(target.key, tn.keyPrefix) {
			child := buildProofInner(t, tn.innerLeaf, target, hasher)
			if child == nil {
				return nil
			}
			branch.innerLeaf = child
		} else if len(longestCommonPrefix(target.key, tn.keyPrefix)) == tn.Len() {
			child := buildProofInner(t, tn.child(target.key), target, hasher)
			if child == nil {
				return nil
			}
			branch.children[target.key[branch.Len()]] = child
		} else {
			// len(longestCommonPrefix(target.key, tn.keyPrefix)) < tn.Len()
			// target does not exist in trie
			return nil
		}

		// Replace children not on main path with MerkleHashNode's
		for i, elem := range branch.children {
			if elem == nil {
				hash, err := hasher.Hash(t, tn.children[i])
				if err != nil {
					// TODO: Log
					return nil
				}
				branch.children[i] = &MerkleHashNode{hash: hash}
			}
		}

		// Replace innerLeaf if not on main path with MerkleHashNode
		if branch.innerLeaf == nil {
			hash, err := hasher.Hash(t, tn.innerLeaf)
			if err != nil {
				// TODO: Log
				return nil
			}
			branch.innerLeaf = &MerkleHashNode{hash: hash}
		}

		hash, err := hasher.Hash(t, tn)
		if err != nil {
			// TODO: Log
			return nil
		}
		branch.setHash(hash)
		return branch
	case nil:
		// target does not exist in trie
		return nil
	default:
		panic(fmt.Sprintf("Invalid node type: %T, %s", tn, tn))
	}

}

func VerifyProof(hash []byte, target *MerkleLeafNode, proof MerkleNode) bool {
	proof = replaceLeaf(target, proof)
	return bytes.Equal(NewHasher().hashNode(proof), hash)
}

func replaceLeaf(leaf *MerkleLeafNode, proof MerkleNode) MerkleNode {
	switch tProof := proof.(type) {
	case *MerkleHashNode:
		return leaf
	case *MerkleBranchNode:
		if bytes.Equal(leaf.key, tProof.keyPrefix) {
			tProof.innerLeaf = replaceLeaf(leaf, tProof.innerLeaf)
		} else if len(longestCommonPrefix(leaf.key, tProof.keyPrefix)) == tProof.Len() {
			child := replaceLeaf(leaf, tProof.child(leaf.key))
			if child == nil {
				return nil
			}
			tProof.setChild(child)
		} else {
			return nil
		}
		return tProof
	default:
		panic(fmt.Sprintf("Invalid node type: %T, %s", tProof, tProof))
	}
}
