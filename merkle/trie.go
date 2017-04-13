package merkle

import "bytes"

const alphabetSize uint = 16

// Patricia hash trie
type MerkleTrie struct {
	root MerkleNode
}

type MerkleNode interface {
	Repr() []byte
	Len() int
}

type MerkleLeafNode struct {
	key []byte
	val interface{}
}

// Node in patricia hash trie
type MerkleBranchNode struct {
	children  [alphabetSize]MerkleNode
	keyPrefix []byte
	innerLeaf *MerkleLeafNode
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
	n.children[node.Repr()[n.Len()]] = node
}

func (t *MerkleTrie) Get(key []byte) interface{} {
	key = hexEncode(key)
	branch := t.closestBranch(key)

	if branch != nil {
		// Checking if branch contains leaf with key
		if branch.Len() == len(key) {
			if branch.innerLeaf != nil && bytes.Equal(key, branch.innerLeaf.key) {
				return branch.innerLeaf.val
			}
		} else if leaf, ok := branch.child(key).(*MerkleLeafNode); ok {
			if bytes.Equal(key, leaf.key) {
				return leaf.val
			}
		}
	} else if leaf, ok := t.root.(*MerkleLeafNode); ok {
		// Checking if root leaf has given key
		if bytes.Equal(key, leaf.key) {
			return leaf.val
		}
	}

	return nil
}

func (t *MerkleTrie) Contains(key []byte) bool {
	return t.Get(key) != nil
}

// Returns whether addition was successful
// It will be unsuccessful if the key already exists in the trie
func (t *MerkleTrie) Add(key []byte, val interface{}) bool {
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
	branch := t.closestBranch(key)
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
			child := branch.child(key)
			if leaf, ok := child.(*MerkleLeafNode); ok {
				// Found leaf that has a common prefix with key longer than the prefix of branch
				otherNode = leaf
				longestPrefix = longestCommonPrefix(key, leaf.key)
			} else if longBranch, ok := child.(*MerkleBranchNode); ok {
				// Found branch that has a common prefix with key longer than the prefix of branch
				otherNode = longBranch
				longestPrefix = longestCommonPrefix(key, longBranch.keyPrefix)
			} else {
				// A branch already exists at the required fork so we simply add the newNode as a
				// child.
				branch.setChild(newNode)
				return true
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
func (t *MerkleTrie) closestBranch(key []byte) *MerkleBranchNode {
	var prevBranch *MerkleBranchNode = nil
	curNode := t.root

	for curNode != nil {
		switch curNode.(type) {
		case *MerkleLeafNode:
			return prevBranch
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
				return branch
			} else {
				// Prefix did not match or prefix is longer than key
				return prevBranch
			}
		}
	}

	return prevBranch
}

func hexEncode(src []byte) []byte {
	dst := make([]byte, 2*len(src))
	for i := 0; i < len(src); i++ {
		dst[2*i] = src[i] >> 4
		dst[2*i+1] = src[i] & 15
	}
	return dst
}
