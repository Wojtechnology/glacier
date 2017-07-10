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
		Sig:       []byte{24},
		VotedAt:   big.NewInt(33),
		PrevBlock: BytesToHash([]byte{43}),
		NextBlock: BytesToHash([]byte{53}),
		Value:     true,
	}
	hash := rlpHash(&voteBody{
		Voter:     v.Voter,
		PrevBlock: v.PrevBlock,
		NextBlock: v.NextBlock,
		Value:     v.Value,
	})

	expected := &meddb.Vote{
		Hash:      hash.Bytes(),
		Voter:     []byte{23},
		Sig:       []byte{24},
		VotedAt:   big.NewInt(33),
		PrevBlock: BytesToHash([]byte{43}).Bytes(), // Pads with zeros
		NextBlock: BytesToHash([]byte{53}).Bytes(), // Pads with zeros
		Value:     true,
	}
	actual := v.toDBVote()
	assert.Equal(t, expected, actual)

	back := fromDBVote(actual)
	assert.Equal(t, v, back)
}

func TestDBVoteMapperEmpty(t *testing.T) {
	v := &Vote{
		PrevBlock: BytesToHash([]byte{43}), // This should never be empty
		NextBlock: BytesToHash([]byte{53}), // This should never be empty
	}
	hash := rlpHash(&voteBody{
		PrevBlock: v.PrevBlock,
		NextBlock: v.NextBlock,
	})

	expected := &meddb.Vote{
		Hash:      hash.Bytes(),
		PrevBlock: BytesToHash([]byte{43}).Bytes(), // Pads with zeros
		NextBlock: BytesToHash([]byte{53}).Bytes(), // Pads with zeros
	}
	actual := v.toDBVote()
	assert.Equal(t, expected, actual)

	back := fromDBVote(actual)
	assert.Equal(t, v, back)
}
