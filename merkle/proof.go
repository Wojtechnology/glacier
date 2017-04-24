package merkle

// Returns proof that a key exists within the trie
func BuildProof(t *MerkleTrie) MerkleNode {
	return nil
}

func VerifyProof(hash, key, value []byte, proof MerkleNode) bool {
	return true
}
