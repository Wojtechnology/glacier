package core

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wojtechnology/glacier/meddb"
)

func TestDBVoteMapper(t *testing.T) {
	v := &Vote{
		Voter:     []byte{23},
		VotedAt:   big.NewInt(33),
		PrevBlock: []byte{43},
		NextBlock: []byte{53},
		Value:     true,
	}
	hash := rlpHash(v)

	expected := &meddb.Vote{
		Hash:      hash.Bytes(),
		Voter:     []byte{23},
		VotedAt:   big.NewInt(33),
		PrevBlock: []byte{43},
		NextBlock: []byte{53},
		Value:     true,
	}
	actual := v.toDBVote()
	assert.Equal(t, expected, actual)

	back := fromDBVote(actual)
	assert.Equal(t, v, back)
}

func TestDBVoteMapperEmpty(t *testing.T) {
	v := &Vote{}
	hash := rlpHash(v)

	expected := &meddb.Vote{
		Hash: hash.Bytes(),
	}
	actual := v.toDBVote()
	assert.Equal(t, expected, actual)

	back := fromDBVote(actual)
	assert.Equal(t, v, back)
}
