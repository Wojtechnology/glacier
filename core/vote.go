package core

import (
	"math/big"

	"github.com/wojtechnology/glacier/meddb"
)

type Vote struct {
	Voter     []byte
	VotedAt   *big.Int
	PrevBlock []byte
	NextBlock []byte // Block we are voting on
	Value     bool
}

// --------
// Vote API
// --------

func (v *Vote) Hash() Hash {
	// TODO: Think about this, maybe we want to hash a subset
	return rlpHash(v)
}

func (v *Vote) toDBVote() *meddb.Vote {
	var votedAt *big.Int = nil
	if v.VotedAt != nil {
		votedAt = big.NewInt(v.VotedAt.Int64())
	}

	return &meddb.Vote{
		Hash:      v.Hash().Bytes(),
		Voter:     v.Voter,
		VotedAt:   votedAt,
		PrevBlock: v.PrevBlock,
		NextBlock: v.NextBlock,
		Value:     v.Value,
	}
}

func fromDBVote(v *meddb.Vote) *Vote {
	var votedAt *big.Int = nil
	if v.VotedAt != nil {
		votedAt = big.NewInt(v.VotedAt.Int64())
	}

	return &Vote{
		Voter:     v.Voter,
		VotedAt:   votedAt,
		PrevBlock: v.PrevBlock,
		NextBlock: v.NextBlock,
		Value:     v.Value,
	}
}
