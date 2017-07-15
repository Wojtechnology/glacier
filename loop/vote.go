package loop

import "github.com/wojtechnology/glacier/core"

const voteBlockBatchSize = 10

type voteLoopState struct {
	prevBlockId core.Hash
}

func newVoteLoopState() *voteLoopState {
	// TODO: Replace with actual genesis block id
	return &voteLoopState{prevBlockId: core.StringToHash("I AM NOT GENESIS")}
}

// TODO: Better map/reduce type abstraction for this.
func VoteOnBlocksLoop(bc *core.Blockchain, errChannel chan<- error) {
	s := newVoteLoopState()
	newestB, err := getMostRecentVotedOnBlock(bc)
	if err != nil {
		errChannel <- err
	}

	if newestB != nil {
		s.prevBlockId = newestB.Hash()
	}

	// TODO: Maybe somehow backfill blocks that were missed when the node was down. Look below.

	cursor, err := bc.GetBlockChangefeed()
	if err != nil {
		errChannel <- err
	}

	var res core.BlockChange
	for cursor.Next(&res) {
		if res.NewBlock != nil && res.OldBlock == nil {
			// Only vote on brand new blocks
			err := voteOnBlock(bc, s, res.NewBlock)
			if err != nil {
				errChannel <- err
			}
		}
	}
}

func voteOnBlock(bc *core.Blockchain, s *voteLoopState, b *core.Block) error {
	valid := true
	err := bc.ValidateBlock(b)
	if err != nil {
		if _, ok := err.(*core.BlockSignatureInvalidError); ok {
			// TODO: Log error
			valid = false
		} else if _, ok := err.(*core.TransactionErrors); ok {
			// TODO: Log error
			valid = false
		} else {
			return err
		}
	}

	v, err := bc.BuildVote(b.Hash(), s.prevBlockId, valid)
	if err != nil {
		return err
	}
	if err := bc.WriteVote(v); err != nil {
		return err
	}
	s.prevBlockId = b.Hash()

	return nil
}

func getMostRecentVotedOnBlock(bc *core.Blockchain) (*core.Block, error) {
	newestVs, err := bc.GetRecentVotes()
	if err != nil {
		return nil, err
	}

	blockIds := make([]core.Hash, len(newestVs))
	for i, v := range newestVs {
		blockIds[i] = v.NextBlock
	}

	// TODO: Error handling here could be done better (i.e. if a block does not exist, it's okay
	// so just ignore it). Rn it will just error out. This needs to be fixed on DB layer.
	bs, err := bc.GetBlocks(blockIds)
	if err != nil {
		return nil, err
	}

	// Find newest block from those voted on at the same time.
	if len(bs) > 0 {
		newestB := bs[0]
		for _, b := range bs[1:] {
			if b.CreatedAt.Int64() > newestB.CreatedAt.Int64() {
				newestB = b
			}
		}
		return newestB, nil
	}
	return nil, nil // No most recent block but also no error
}

// Old way of voting, still might be useful when we write logic to backfill votes in case the node
// goes down for some time. Not worried about that for now.
//
// Comment from before:
// This is a really shitty way to do this. It's possible that there is clock drift between
// computers and there is a race condition:
//     This node votes on block b1 with CreatedAt = 123 NS. Very soon after, another node
//     creates a block b2 with CreatedAt = 122 NS. That block can no longer be voted on by
//     this node ever again.
// Two solutions:
// 1) Use changefeed (best solution, one used by bigchaindb).
// 2) Decrease granularity of timestamps to seconds (doesn't really remove the issue but makes
//    it smaller).
// We will still need something like this logic in case a node goes down and wants to "catch up" on
// blocks that it missed while it was down.
// Alas, this is the way it works for now, but as soon as changefeeds are implemented, we are using
// those.
func voteOnBlocksBatched(bc *core.Blockchain) error {
	var (
		newestTS    int64
		prevBlockId core.Hash
	)
	newestB, err := getMostRecentVotedOnBlock(bc)
	if err != nil {
		return err
	}

	// Find newest block from those voted on at the same time.
	if newestB != nil {
		newestTS = newestB.CreatedAt.Int64()
		prevBlockId = newestB.Hash()
	} else {
		// This is the first time voting.
		newestTS = 0
		// TODO: This should actually be the genesis block.
		prevBlockId = core.BytesToHash([]byte("I AM NOT GENESIS"))
	}

	// Note that these are sorted by increasing CreatedAt.
	// TODO: This call should exclude the above blockIds instead of adding +1. Requires change on
	// DB layer.
	bs, err := bc.GetOldestBlocks(newestTS+1, voteBlockBatchSize)
	if err != nil {
		return err
	}
	for _, b := range bs {
		valid := true
		// TODO: Validate Transactions
		// TODO: Validate Block
		v, err := bc.BuildVote(b.Hash(), prevBlockId, valid)
		if err != nil {
			return err
		}
		if err := bc.WriteVote(v); err != nil {
			return err
		}
		prevBlockId = b.Hash()
	}

	return nil
}
