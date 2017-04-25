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
	innerLeaf MerkleNode
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
	return t.getInner(t.root, hexEncode(key))
}

func (t *MerkleTrie) getInner(n MerkleNode, key []byte) interface{} {
	n, err := t.maybeResolveNode(n)
	if err != nil {
		// TODO: Log
		return nil
	}

	switch tn := n.(type) {
	case *MerkleLeafNode:
		if bytes.Equal(key, tn.key) {
			return tn.val
		} else {
			return nil
		}
	case *MerkleBranchNode:
		if bytes.Equal(key, tn.keyPrefix) {
			inner, err := t.maybeResolveNode(tn.innerLeaf)
			if err != nil {
				// TODO: Log
				return nil
			}

			switch tInner := inner.(type) {
			case *MerkleLeafNode:
				return tInner.val
			case nil:
				return nil
			default:
				panic(fmt.Sprintf("Invalid inner node type: %T, %s", tInner, tInner))
			}
		} else if len(longestCommonPrefix(key, tn.keyPrefix)) == tn.Len() {
			child := tn.child(key)
			if child != nil {
				return t.getInner(child, key)
			} else {
				return nil
			}
		} else {
			// len(longestCommonPrefix(key, tn.keyPrefix)) < tn.Len()
			return nil
		}
	case nil:
		return nil
	default:
		panic(fmt.Sprintf("Invalid node type: %T, %s", tn, tn))
	}
}

// Returns whether the trie contains given key
func (t *MerkleTrie) Contains(key []byte) bool {
	return t.Get(key) != nil
}

// Returns whether addition was successful
// It will be unsuccessful if the key already exists in the trie
func (t *MerkleTrie) Add(key []byte, val interface{}) error {
	key = hexEncode(key)
	if t.root == nil {
		t.root = &MerkleLeafNode{key: key, val: val}
		return nil
	}
	return t.addInner(t.root, nil, key, val)
}

// Recursive helper method for adding a node to the trie
func (t *MerkleTrie) addInner(n MerkleNode, prevBranch *MerkleBranchNode, key []byte,
	val interface{}) error {
	var err error
	n, err = t.maybeResolveNode(n)
	if err != nil {
		// TODO: return error here
		return err
	}

	switch tn := n.(type) {
	case *MerkleLeafNode:
		if bytes.Equal(key, tn.key) {
			// TODO: return error here
			return AlreadyExistsError{Key: key, Node: n}
		}
		t.forkNode(tn, prevBranch, key, val)
		return nil
	case *MerkleBranchNode:
		if bytes.Equal(key, tn.keyPrefix) {
			inner, err := t.maybeResolveNode(tn.innerLeaf)
			if err != nil {
				return err
			}

			switch tInner := inner.(type) {
			case *MerkleLeafNode:
				return AlreadyExistsError{Key: key, Node: n}
			case nil:
				tn.innerLeaf = &MerkleLeafNode{key: key, val: val}
				return nil
			default:
				panic(fmt.Sprintf("Invalid inner node type: %T, %s", tInner, tInner))
			}
		} else if len(longestCommonPrefix(key, tn.keyPrefix)) == tn.Len() {
			child := tn.child(key)
			if child != nil {
				return t.addInner(child, tn, key, val)
			} else {
				tn.setChild(&MerkleLeafNode{key: key, val: val})
				return nil
			}
		} else {
			// len(longestCommonPrefix(key, tn.keyPrefix)) < tn.Len()
			t.forkNode(tn, prevBranch, key, val)
			return nil
		}
	default:
		panic(fmt.Sprintf("Invalid node type: %T, %s", tn, tn))
	}
}

// Helper to create a fork between n and the new node with key and value provided
func (t *MerkleTrie) forkNode(n MerkleNode, prevBranch *MerkleBranchNode, key []byte,
	val interface{}) {
	_, isBranch := n.(*MerkleBranchNode)
	_, isLeaf := n.(*MerkleLeafNode)
	if !(isBranch || isLeaf) {
		panic(fmt.Sprintf("Invalid node type for forking: %T, %s", n, n))
	}

	longestPrefix := longestCommonPrefix(key, n.Repr())
	fork := &MerkleBranchNode{keyPrefix: longestPrefix}
	newNode := &MerkleLeafNode{key: key, val: val}

	if fork.Len() == newNode.Len() {
		fork.innerLeaf = newNode
	} else {
		fork.setChild(newNode)
	}

	if leaf, ok := n.(*MerkleLeafNode); ok && fork.Len() == n.Len() {
		// Since we check otherNode is not the same as newNode, this will not happen if newNode
		// was set as innerLeaf
		fork.innerLeaf = leaf
	} else {
		// If n is a branch, we know that it has a longer prefix than fork
		fork.setChild(n)
	}

	if prevBranch != nil {
		prevBranch.setChild(fork)
	} else {
		t.root = fork
	}
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

// Returns hex encoding of byte array
func hexEncode(src []byte) []byte {
	dst := make([]byte, 2*len(src))
	for i := 0; i < len(src); i++ {
		dst[2*i] = src[i] >> 4
		dst[2*i+1] = src[i] & 15
	}
	return dst
}
