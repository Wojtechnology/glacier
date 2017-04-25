package merkle

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/test"
)

func buildTrie() *MerkleTrie {
	trie := &MerkleTrie{}

	leaf := &MerkleLeafNode{
		key: []byte{0, 0, 1, 0, 0, 2, 0, 3, 0, 4},
		val: "someValue", // []byte{0, 16, 2, 3, 4}
	}
	otherLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 2, 0}, // []byte{0, 32}
		val: "someOtherValue",
	}
	innerLeaf := &MerkleLeafNode{
		key: []byte{0, 0, 1, 0, 0, 2}, // []byte{0, 16, 2}
		val: "someValueInner",
	}

	innerBranch := &MerkleBranchNode{
		keyPrefix: []byte{0, 0, 1, 0, 0, 2}, // []byte{0, 16, 2}
		innerLeaf: innerLeaf,
	}
	innerBranch.children[0] = leaf
	branch := &MerkleBranchNode{
		keyPrefix: []byte{0, 0}, // []byte{0}
	}
	branch.children[1] = innerBranch
	branch.children[2] = otherLeaf
	trie.root = branch

	return trie
}

func buildRootLeafTrie() *MerkleTrie {
	trie := &MerkleTrie{}

	leaf := &MerkleLeafNode{
		key: []byte{0, 0, 0, 1, 0, 2}, // []byte{0, 1, 2}
		val: "someValue",
	}
	trie.root = leaf

	return trie
}

func buildEmptyTrie() *MerkleTrie {
	return &MerkleTrie{}
}

// -------------
// Test Contains
// -------------

func TestDoesNotContainEmpty(t *testing.T) {
	trie := buildTrie()
	test.AssertEqual(t, false, trie.Contains([]byte{}))
}

func TestDoesNotContainInnerLeaf(t *testing.T) {
	trie := buildTrie()
	test.AssertEqual(t, false, trie.Contains([]byte{0}))
}

func TestContainsInnerLeaf(t *testing.T) {
	trie := buildTrie()
	test.AssertEqual(t, true, trie.Contains([]byte{0, 16, 2}))
}

func TestContainsLeaf(t *testing.T) {
	trie := buildTrie()
	test.AssertEqual(t, true, trie.Contains([]byte{0, 16, 2, 3, 4}))
}

// Longer than leaf
func TestDoesNotContainLong1(t *testing.T) {
	trie := buildTrie()
	test.AssertEqual(t, false, trie.Contains([]byte{0, 16, 2, 3, 4, 5}))
}

// Longer than branch
func TestDoesNotContainLong2(t *testing.T) {
	trie := buildTrie()
	test.AssertEqual(t, false, trie.Contains([]byte{0, 16, 2, 16}))
}

func TestDoesNotContainMissingChild(t *testing.T) {
	trie := buildTrie()
	test.AssertEqual(t, false, trie.Contains([]byte{0, 16, 4}))
}

func TestDoesNotContainRootLeaf(t *testing.T) {
	trie := buildRootLeafTrie()
	test.AssertEqual(t, false, trie.Contains([]byte{0, 1}))
	test.AssertEqual(t, false, trie.Contains([]byte{0, 1, 2, 3}))
}

func TestContainsRootLeaf(t *testing.T) {
	trie := buildRootLeafTrie()
	test.AssertEqual(t, true, trie.Contains([]byte{0, 1, 2}))
}

func TestContainsEmptyTrie(t *testing.T) {
	trie := buildEmptyTrie()
	test.AssertEqual(t, false, trie.Contains([]byte{}))
	test.AssertEqual(t, false, trie.Contains([]byte{1}))
}

// ----------------
// Test Add and Get
// ----------------

func TestAddToEmpty(t *testing.T) {
	trie := buildEmptyTrie()
	key := []byte{1}
	val := "someValue"

	assert.Nil(t, trie.Add(key, val))
	test.AssertEqual(t, val, trie.Get(key))
}

func TestAddBranchToEmpty(t *testing.T) {
	trie := buildEmptyTrie()
	key1 := []byte{1, 2}
	val1 := "someValue1"
	key2 := []byte{1, 4}
	val2 := "someValue2"

	assert.Nil(t, trie.Add(key1, val1))
	assert.Nil(t, trie.Add(key2, val2))
	test.AssertEqual(t, val1, trie.Get(key1))
	test.AssertEqual(t, val2, trie.Get(key2))
}

func TestAddEmptyBranchToEmpty(t *testing.T) {
	trie := buildEmptyTrie()
	key1 := []byte{1}
	val1 := "someValue1"
	key2 := []byte{16}
	val2 := "someValue2"

	assert.Nil(t, trie.Add(key1, val1))
	assert.Nil(t, trie.Add(key2, val2))
	test.AssertEqual(t, val1, trie.Get(key1))
	test.AssertEqual(t, val2, trie.Get(key2))
}

func testAddAndGet(t *testing.T, trie *MerkleTrie, key []byte) {
	val := "someValue"
	assert.Nil(t, trie.Add(key, val))
	test.AssertEqual(t, val, trie.Get(key))
}

func TestAddInnerLeaf(t *testing.T) {
	testAddAndGet(t, buildTrie(), []byte{0})
}

func TestAddToNewBranch(t *testing.T) {
	testAddAndGet(t, buildTrie(), []byte{0, 16, 3, 4})
}

// New node becomes inner leaf
func TestAddToNewBranchInnerLeaf1(t *testing.T) {
	testAddAndGet(t, buildTrie(), []byte{0, 16})
}

// Existing node becomes inner leaf
func TestAddToNewBranchInnerLeaf2(t *testing.T) {
	testAddAndGet(t, buildTrie(), []byte{0, 16, 2, 3, 4, 5})
}

func TestAddToExistingBranch(t *testing.T) {
	testAddAndGet(t, buildTrie(), []byte{0, 1, 2, 3})
}

func testAddAlreadyExists(t *testing.T, trie *MerkleTrie, key []byte) {
	val := "someValue"
	err := trie.Add(key, val)
	assert.NotNil(t, err)
	assert.IsType(t, AlreadyExistsError{}, err)
}

func TestAddAlreadyExistsRootLeaf(t *testing.T) {
	testAddAlreadyExists(t, buildRootLeafTrie(), []byte{0, 1, 2})
}

func TestAddAlreadyExistsLeaf(t *testing.T) {
	testAddAlreadyExists(t, buildTrie(), []byte{0, 16, 2, 3, 4})
}

func TestAddAlreadyInnerLeaf(t *testing.T) {
	testAddAlreadyExists(t, buildTrie(), []byte{0, 16, 2})
}

// -----------------
// Test Hex Encoding
// -----------------

func TestHexEncode(t *testing.T) {
	test.AssertEqual(t, []byte{4, 2, 9, 6, 0, 0, 1, 0, 0, 1}, hexEncode([]byte{66, 150, 0, 16, 1}))
}

func TestHexEncodeEmpty(t *testing.T) {
	test.AssertEqual(t, []byte{}, hexEncode([]byte{}))
}
