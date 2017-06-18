package loop

import (
	"time"

	"github.com/wojtechnology/glacier/core"
)

const (
	voteLoopWaitMS     = 1000
	voteBlockBatchSize = 10
)

func VoteOnBlocksLoop(bc *core.Blockchain, errChannel chan<- error) {
	for true {
		err := voteOnBlocks(bc)
		if err != nil {
			errChannel <- err
		}
		// TODO: Adjust for time spent
		timeChannel := time.After(time.Millisecond * voteLoopWaitMS)
		<-timeChannel
	}
}

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
func voteOnBlocks(bc *core.Blockchain) error {
	newestVs, err := bc.GetRecentVotes()
	if err != nil {
		return err
	}

	blockIds := make([]core.Hash, len(newestVs))
	for i, v := range newestVs {
		blockIds[i] = v.NextBlock
	}

	// TODO: Error handling here could be done better (i.e. if a block does not exist, it's okay
	// so just ignore it). Rn it will just error out. This needs to be fixed on DB layer.
	bs, err := bc.GetBlocks(blockIds)
	if err != nil {
		return err
	}

	var (
		newestTS    int64
		prevBlockId core.Hash
	)

	// Find newest block from those voted on at the same time.
	if len(bs) > 0 {
		newestB := bs[0]
		for _, b := range bs[1:] {
			if b.CreatedAt.Int64() > newestB.CreatedAt.Int64() {
				newestB = b
			}
		}
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
	bs, err = bc.GetOldestBlocks(newestTS+1, voteBlockBatchSize)
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
