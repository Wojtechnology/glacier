package merkle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Invalid key when trying to match with leaf
func TestBuildProofNonExistent1(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)
	missingLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 1, 0, 0, 2, 0, 3, 0, 4, 0, 5},
		val: leaf.val,
	}

	assert.Nil(t, BuildProof(trie, missingLeaf))
}

// Invalid val when trying to match with leaf
func TestBuildProofNonExistent2(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)
	missingLeaf := &MerkleLeafNode{
		key: leaf.key,
		val: "someValueThatMustBeDifferent",
	}

	assert.Nil(t, BuildProof(trie, missingLeaf))
}

// Reach nil
func TestBuildProofNonExistent3(t *testing.T) {
	trie := buildTrie()
	missingLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 4, 0},
		val: "someValue",
	}

	assert.Nil(t, BuildProof(trie, missingLeaf))
}

// In between branches
func TestBuildProofNonExistent4(t *testing.T) {
	trie := buildTrie()
	missingLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 1, 0},
		val: "someValue",
	}

	assert.Nil(t, BuildProof(trie, missingLeaf))
}

// Invalid val in inner leaf
func TestBuildProofNonExistent5(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	innerLeaf := innerBranch.innerLeaf.(*MerkleLeafNode)
	missingLeaf := &MerkleLeafNode{
		key: innerLeaf.key,
		val: "someValueThatMustBeDifferent",
	}

	assert.Nil(t, BuildProof(trie, missingLeaf))
}

func TestProve(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)

	hash, _ := NewHasher().Hash(trie, trie.root)
	proof := BuildProof(trie, leaf)
	assert.True(t, VerifyProof(hash, leaf, proof))
}

func TestProveInnerLeaf(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	innerLeaf := innerBranch.innerLeaf.(*MerkleLeafNode)

	hash, _ := NewHasher().Hash(trie, trie.root)
	proof := BuildProof(trie, innerLeaf)
	assert.True(t, VerifyProof(hash, innerLeaf, proof))
}

func TestProveInvalidLeaf(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)
	invalidLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 1, 0},
		val: "someValue",
	}

	hash, _ := NewHasher().Hash(trie, trie.root)
	proof := BuildProof(trie, leaf)
	assert.False(t, VerifyProof(hash, invalidLeaf, proof))
}

func TestProveInvalidInnerLeaf(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)
	invalidLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 1, 0},
		val: "someValue",
	}

	hash, _ := NewHasher().Hash(trie, trie.root)
	proof := BuildProof(trie, leaf)
	assert.False(t, VerifyProof(hash, invalidLeaf, proof))
}
