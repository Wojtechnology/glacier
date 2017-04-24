package merkle

import (
	"bytes"
	"fmt"
)

// Patricia hash trie
type MerkleTrie struct {
	root MerkleNode
}

type MerkleNode interface {
	Repr() []byte
	Len() int
}

type MerkleLeafNode struct {
	key   []byte
	val   interface{}
	cache *HashCache
}

// Node in patricia hash trie
type MerkleBranchNode struct {
	children  [16]MerkleNode
	keyPrefix []byte
	innerLeaf *MerkleLeafNode
	cache     *HashCache
}

type MerkleHashNode struct {
	hash []byte
}

type HashCache struct {
	dirty bool
	hash  []byte
}

func (n *MerkleLeafNode) Repr() []byte {
	return n.key
}

func (n *MerkleLeafNode) Len() int {
	return len(n.key)
}

func (n *MerkleBranchNode) Repr() []byte {
	return n.keyPrefix
}

func (n *MerkleBranchNode) Len() int {
	return len(n.keyPrefix)
}

func (n *MerkleBranchNode) child(key []byte) MerkleNode {
	return n.children[key[n.Len()]]
}

func (n *MerkleBranchNode) setChild(node MerkleNode) {
	elem := &n.children[node.Repr()[n.Len()]]
	if *elem != node {
		*elem = node
		if n.cache != nil {
			n.cache.dirty = true
		}
	}
}

func (n *MerkleHashNode) Repr() []byte {
	// Just defined to fit the interface
	return n.hash
}

func (n *MerkleHashNode) Len() int {
	// Just defined to fit the interface
	return len(n.hash)
}

// Returns the value stored at key, nil if the key is not in the trie
func (t *MerkleTrie) Get(key []byte) interface{} {
	key = hexEncode(key)
	branch, err := t.closestBranch(key)
	if err != nil {
		// TODO: Log
		return nil
	}

	var val interface{} = nil
	if branch != nil {
		if branch.Len() == len(key) {
			// Checking if branch contains leaf with key
			val, err = t.getValue(branch, key)
			if err != nil {
				return nil
			}
		} else {
			val, err = t.getValue(branch.child(key), key)
			if err != nil {
				return nil
			}
		}
	} else {
		val, err = t.getValue(t.root, key)
		if err != nil {
			return nil
		}
	}

	return val
}

// Returns whether the trie contains given key
func (t *MerkleTrie) Contains(key []byte) bool {
	return t.Get(key) != nil
}

// Returns whether addition was successful
// It will be unsuccessful if the key already exists in the trie
func (t *MerkleTrie) Add(key []byte, val interface{}) bool {
	// TODO: maybe return error instead of bool
	key = hexEncode(key)
	newNode := &MerkleLeafNode{
		key: key,
		val: val,
	}

	if t.root == nil {
		t.root = newNode
		return true
	}

	var longestPrefix []byte
	var otherNode MerkleNode
	branch, err := t.closestBranch(key)
	if err != nil {
		// TODO: Log and if you change this function to return error, return the error here
		return false
	}

	if branch != nil {
		if branch.Len() == len(key) {
			if branch.innerLeaf != nil {
				// Inner leaf matching key already exists
				return false
			}

			// Adding as inner leaf
			branch.innerLeaf = newNode
			return true
		} else {
			// Prefix of branch was shorter than key
			var child MerkleNode
			child, err = t.maybeResolveNode(branch.child(key))
			if err != nil {
				// TODO: Log and if you change this function to return error, return the error here
				return false
			}

			switch tChild := child.(type) {
			case *MerkleLeafNode:
				// Found leaf that has a common prefix with key longer than the prefix of branch
				otherNode = tChild
				longestPrefix = longestCommonPrefix(key, tChild.key)
			case *MerkleBranchNode:
				// Found branch that has a common prefix with key longer than the prefix of branch
				otherNode = tChild
				longestPrefix = longestCommonPrefix(key, tChild.keyPrefix)
			case nil:
				// A branch already exists at the required fork so we simply add the newNode as a
				// child.
				branch.setChild(newNode)
				return true
			default:
				panic(fmt.Sprintf("Invalid node type: %T, %s", tChild, tChild))
			}
		}
	} else {
		otherNode = t.root
		longestPrefix = longestCommonPrefix(key, t.root.Repr())
	}

	if leaf, ok := otherNode.(*MerkleLeafNode); ok && bytes.Equal(key, leaf.key) {
		// Leaf with this key already exists
		return false
	}

	// If we are here, it means that we are creating a new fork between an existing node and the
	// new leaf
	newBranch := &MerkleBranchNode{
		keyPrefix: longestPrefix,
	}

	if newBranch.Len() == newNode.Len() {
		newBranch.innerLeaf = newNode
	} else {
		newBranch.setChild(newNode)
	}

	if leaf, ok := otherNode.(*MerkleLeafNode); ok && newBranch.Len() == otherNode.Len() {
		// Since we check otherNode is not the same as newNode, this will not happen if newNode
		// was set as innerLeaf
		newBranch.innerLeaf = leaf
	} else {
		// If otherNode is a branch, we know that it has a longer prefix than newBranch
		newBranch.setChild(otherNode)
	}

	if branch != nil {
		branch.setChild(newBranch)
	} else {
		t.root = newBranch
	}

	return true
}

// Returns minimum of two integers
func min(first, second int) int {
	if first < second {
		return first
	}
	return second
}

// Returns longest common prefix of two byte arrays
func longestCommonPrefix(first, second []byte) []byte {
	var i int
	for i = 0; i < min(len(first), len(second)); i++ {
		if first[i] != second[i] {
			break
		}
	}
	return first[:i]
}

// Returns the closest branch node that has a keyPrefix length less than or equal to key
func (t *MerkleTrie) closestBranch(key []byte) (*MerkleBranchNode, error) {
	var prevBranch *MerkleBranchNode = nil
	curNode := t.root

	for curNode != nil {
		var err error
		curNode, err = t.maybeResolveNode(curNode)
		if err != nil {
			// TODO: Log
			return nil, err
		}

		switch tn := curNode.(type) {
		case *MerkleLeafNode:
			return prevBranch, nil
		case *MerkleBranchNode:
			branch := curNode.(*MerkleBranchNode)

			if branch.Len() < len(key) &&
				bytes.Equal(branch.keyPrefix, key[:branch.Len()]) {
				// BranchNode with strict matching prefix found, keep searching
				prevBranch = branch
				curNode = branch.child(key)
			} else if branch.Len() == len(key) &&
				bytes.Equal(branch.keyPrefix, key) {
				// Exact BranchNode found
				return branch, nil
			} else {
				// Prefix did not match or prefix is longer than key
				return prevBranch, nil
			}
		default:
			panic(fmt.Sprintf("Invalid node type: %T, %s", tn, tn))
		}
	}

	return prevBranch, nil
}

func (t *MerkleTrie) maybeResolveNode(n MerkleNode) (MerkleNode, error) {
	if hashNode, ok := n.(*MerkleHashNode); ok {
		// TODO: When we actually write nodes to db, implement this
		return t.resolveNode(hashNode)
	}
	return n, nil
}

func (t *MerkleTrie) resolveNode(n *MerkleHashNode) (MerkleNode, error) {
	return nil, fmt.Errorf("Not implemented error: cannot resolve node for MerkleHashNode")
}

// Returns the value that the node contains if it contains anything, nil otherwise
func (t *MerkleTrie) getValue(n MerkleNode, key []byte) (interface{}, error) {
	var err error
	n, err = t.maybeResolveNode(n)
	if err != nil {
		return false, nil
	}

	switch tn := n.(type) {
	case *MerkleBranchNode:
		if tn.innerLeaf != nil && bytes.Equal(key, tn.innerLeaf.key) {
			return tn.innerLeaf.val, nil
		}
	case *MerkleLeafNode:
		if bytes.Equal(key, tn.key) {
			return tn.val, nil
		}
	default:
		return false, fmt.Errorf("Invalid node type: %T, %s", tn, tn)
	}
	return nil, nil
}

// Returns hex encoding of byte array
func hexEncode(src []byte) []byte {
	dst := make([]byte, 2*len(src))
	for i := 0; i < len(src); i++ {
		dst[2*i] = src[i] >> 4
		dst[2*i+1] = src[i] & 15
	}
	return dst
}
