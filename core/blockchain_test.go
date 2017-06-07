package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomAssignee(t *testing.T) {
	node := &Node{PubKey: []byte{42}}
	otherNode := &Node{PubKey: []byte{43}}

	bc := &Blockchain{federation: []*Node{node, otherNode}}
	randNode := bc.randomAssignee(0)
	assert.Equal(t, node, randNode)
	randNode = bc.randomAssignee(1)
	assert.Equal(t, otherNode, randNode)
}
