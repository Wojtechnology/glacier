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

	proof, err := BuildProof(trie, missingLeaf)
	assert.NotNil(t, err)
	assert.IsType(t, NotFoundError{}, err)
	assert.Nil(t, proof)
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

	proof, err := BuildProof(trie, missingLeaf)
	assert.NotNil(t, err)
	assert.IsType(t, NotFoundError{}, err)
	assert.Nil(t, proof)
}

// Reach nil
func TestBuildProofNonExistent3(t *testing.T) {
	trie := buildTrie()
	missingLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 4, 0},
		val: "someValue",
	}

	proof, err := BuildProof(trie, missingLeaf)
	assert.NotNil(t, err)
	assert.IsType(t, NotFoundError{}, err)
	assert.Nil(t, proof)
}

// In between branches
func TestBuildProofNonExistent4(t *testing.T) {
	trie := buildTrie()
	missingLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 1, 0},
		val: "someValue",
	}

	proof, err := BuildProof(trie, missingLeaf)
	assert.NotNil(t, err)
	assert.IsType(t, NotFoundError{}, err)
	assert.Nil(t, proof)
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

	proof, err := BuildProof(trie, missingLeaf)
	assert.NotNil(t, err)
	assert.IsType(t, NotFoundError{}, err)
	assert.Nil(t, proof)
}

func TestProve(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)

	hash := NewHasher().hash(trie.root)
	proof, err := BuildProof(trie, leaf)
	assert.Nil(t, err)
	assert.True(t, VerifyProof(hash, leaf, proof))
}

func TestProveInnerLeaf(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	innerLeaf := innerBranch.innerLeaf.(*MerkleLeafNode)

	hash := NewHasher().hash(trie.root)
	proof, err := BuildProof(trie, innerLeaf)
	assert.Nil(t, err)
	assert.True(t, VerifyProof(hash, innerLeaf, proof))
}

func TestProveInvalidPath(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)
	invalidLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 1, 0},
		val: "someValue",
	}

	hash := NewHasher().hash(trie.root)
	proof, err := BuildProof(trie, leaf)
	assert.Nil(t, err)
	assert.False(t, VerifyProof(hash, invalidLeaf, proof))
}

func TestProveInvalidBranchHash(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)

	hash := NewHasher().hash(trie.root)
	proof, err := BuildProof(trie, leaf)
	assert.Nil(t, err)
	proof[0].children[1] = &MerkleHashNode{hash: []byte{4, 2}}
	assert.False(t, VerifyProof(hash, leaf, proof))
}

func TestProveInvalidLeafHash(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)
	invalidLeaf := &MerkleLeafNode{
		key: leaf.key,
		val: "someValueThatMustBeDifferentThanLeaf",
	}

	hash := NewHasher().hash(trie.root)
	proof, err := BuildProof(trie, leaf)
	assert.Nil(t, err)
	assert.False(t, VerifyProof(hash, invalidLeaf, proof))
}
