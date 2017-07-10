package core

import (
	"math/big"

	"github.com/wojtechnology/glacier/meddb"
)

type Vote struct {
	Voter     []byte
	Sig       []byte
	VotedAt   *big.Int
	PrevBlock Hash
	NextBlock Hash // Block we are voting on
	Value     bool
}

// --------
// Vote API
// --------

type voteBody struct {
	Voter     []byte
	PrevBlock Hash
	NextBlock Hash
	Value     bool
}

func (v *Vote) Hash() Hash {
	return rlpHash(&voteBody{
		Voter:     v.Voter,
		PrevBlock: v.PrevBlock,
		NextBlock: v.NextBlock,
		Value:     v.Value,
	})
}

func (v *Vote) toDBVote() *meddb.Vote {
	var votedAt *big.Int = nil
	if v.VotedAt != nil {
		votedAt = big.NewInt(v.VotedAt.Int64())
	}

	return &meddb.Vote{
		Hash:      v.Hash().Bytes(),
		Voter:     v.Voter,
		Sig:       v.Sig,
		VotedAt:   votedAt,
		PrevBlock: v.PrevBlock.Bytes(),
		NextBlock: v.NextBlock.Bytes(),
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
		Sig:       v.Sig,
		VotedAt:   votedAt,
		PrevBlock: BytesToHash(v.PrevBlock),
		NextBlock: BytesToHash(v.NextBlock),
		Value:     v.Value,
	}
}

func fromDBVotes(dbVs []*meddb.Vote) []*Vote {
	vs := make([]*Vote, len(dbVs))
	for i, dbV := range dbVs {
		vs[i] = fromDBVote(dbV)
	}
	return vs
}
