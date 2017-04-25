package merkle

import (
	"golang.org/x/crypto/sha3"
	"hash"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/test"
)

func hashLeaf(n *MerkleLeafNode, h hash.Hash) []byte {
	return hashObject(merkleLeafData{key: n.key, val: n.val}, h)
}

func nilSlice(size int) [][]byte {
	s := make([][]byte, size)
	for i := 0; i < size; i++ {
		s[i] = nilHash
	}
	return s
}

func TestHash(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	otherLeaf := branch.children[2].(*MerkleLeafNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)
	innerLeaf := innerBranch.innerLeaf.(*MerkleLeafNode)

	h := sha3.New256()
	var (
		branchHash      []byte
		innerBranchHash []byte
		otherLeafHash   []byte = hashLeaf(otherLeaf, h)
		leafHash        []byte = hashLeaf(leaf, h)
		innerLeafHash   []byte = hashLeaf(innerLeaf, h)
	)

	// Remove caches
	branch.cache = nil
	innerBranch.cache = nil
	leaf.cache = nil
	otherLeaf.cache = nil
	innerLeaf.cache = nil

	// Calculate branch hashes
	s := nilSlice(17)
	s[0] = leafHash
	s[16] = innerLeafHash
	innerBranchHash = hashObject(s, h)

	s = nilSlice(17)
	s[1] = innerBranchHash
	s[2] = otherLeafHash
	branchHash = hashObject(s, h)

	hasher := NewHasher()
	res, err := hasher.Hash(trie, trie.root)
	assert.Nil(t, err)
	test.AssertBytesEqual(t, branchHash, res)

	test.AssertBytesEqual(t, leafHash, leaf.hash())
	test.AssertBytesEqual(t, innerLeafHash, innerLeaf.hash())
	test.AssertBytesEqual(t, otherLeafHash, otherLeaf.hash())
	test.AssertBytesEqual(t, innerBranchHash, innerBranch.hash())
	test.AssertBytesEqual(t, branchHash, branch.hash())

	assert.False(t, leaf.cache.dirty)
	assert.False(t, innerLeaf.cache.dirty)
	assert.False(t, otherLeaf.cache.dirty)
	assert.False(t, innerBranch.cache.dirty)
	assert.False(t, branch.cache.dirty)
}

func TestHashPartial(t *testing.T) {
	trie := buildTrie()
	branch := trie.root.(*MerkleBranchNode)
	innerBranch := branch.children[1].(*MerkleBranchNode)
	otherLeaf := branch.children[2].(*MerkleLeafNode)
	leaf := innerBranch.children[0].(*MerkleLeafNode)
	innerLeaf := innerBranch.innerLeaf.(*MerkleLeafNode)

	h := sha3.New256()
	var (
		innerBranchHash  []byte
		leafHash         []byte = hashLeaf(leaf, h)
		innerLeafHash    []byte = []byte{4, 2}
		hashNodeHash     []byte = []byte{9, 6}
		innerBranch2Hash []byte = []byte{3, 7}
	)

	hashNode := &MerkleHashNode{hash: hashNodeHash}
	innerBranch2 := &MerkleBranchNode{cache: &HashCache{dirty: false, hash: innerBranch2Hash}}
	innerBranch.children[1] = hashNode
	innerBranch.children[2] = innerBranch2

	// Remove caches
	branch.cache = &HashCache{dirty: true}
	innerBranch.cache = nil
	leaf.cache = nil
	otherLeaf.cache = nil
	innerLeaf.cache = &HashCache{dirty: false, hash: innerLeafHash}

	// Calculate branch hashes
	s := nilSlice(17)
	s[0] = leafHash
	s[1] = hashNodeHash
	s[2] = innerBranch2Hash
	s[16] = innerLeafHash
	innerBranchHash = hashObject(s, h)

	hasher := NewHasher()
	res, err := hasher.Hash(trie, innerBranch)
	assert.Nil(t, err)
	test.AssertBytesEqual(t, innerBranchHash, res)

	test.AssertBytesEqual(t, leafHash, leaf.hash())
	test.AssertBytesEqual(t, innerLeafHash, innerLeaf.hash())
	test.AssertBytesEqual(t, innerBranchHash, innerBranch.hash())
	test.AssertBytesEqual(t, innerBranch2Hash, innerBranch2.hash())
	assert.Nil(t, otherLeaf.hash())
	assert.Nil(t, branch.hash())

	assert.False(t, leaf.cache.dirty)
	assert.False(t, innerLeaf.cache.dirty)
	assert.False(t, innerBranch.cache.dirty)
	assert.False(t, innerBranch2.cache.dirty)
	assert.Nil(t, otherLeaf.cache)
	assert.True(t, branch.cache.dirty)
}
