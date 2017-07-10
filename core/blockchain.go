package core

import (
	"math/big"
	"math/rand"

	"github.com/wojtechnology/glacier/common"
	"github.com/wojtechnology/glacier/crypto"
	"github.com/wojtechnology/glacier/meddb"
)

type Blockchain struct {
	db         meddb.BlockchainDB // Stores db data structures
	bt         meddb.Bigtable     // Bigtable that stores cells
	me         *Node              // This node
	federation []*Node            // All other nodes in the network
	// TODO: Federation lock
}

func NewBlockchain(db meddb.BlockchainDB, bt meddb.Bigtable,
	me *Node, federation []*Node) *Blockchain {

	return &Blockchain{
		db:         db,
		bt:         bt,
		me:         me,
		federation: federation,
	}
}

// --------------
// Blockchain API
// --------------

// Adds transaction to blockchain backlog.
func (bc *Blockchain) AddTransaction(tx *Transaction) error {
	now := common.Now()
	tx.AssignedTo = bc.randomAssignee(now).PubKey
	tx.AssignedAt = big.NewInt(now)

	if err := bc.db.WriteTransaction(tx.toDBTransaction()); err != nil {
		return err
	}

	return nil
}

// Returns list of transactions currently assigned to this node in the backlog.
func (bc *Blockchain) GetMyTransactions() ([]*Transaction, error) {
	dbTxs, err := bc.db.GetAssignedTransactions(bc.me.PubKey)
	if err != nil {
		return nil, err
	}
	return fromDBTransactions(dbTxs), nil
}

// Returns list of transactions that are at least staleAge old from backlog.
func (bc *Blockchain) GetStaleTransactions(staleAge int64) ([]*Transaction, error) {
	dbTxs, err := bc.db.GetStaleTransactions(common.Now() - staleAge)
	if err != nil {
		return nil, err
	}
	return fromDBTransactions(dbTxs), nil
}

// Validates transaction.
// Returns nil when validation is successful, returns error with reason otherwise.
func (bc *Blockchain) ValidateTransaction(tx *Transaction) error {
	linkedOutputIds := make([][]byte, len(tx.Inputs))
	for i, input := range tx.Inputs {
		linkedOutputIds[i] = input.OutputHash().Bytes()
	}

	ruleset, err := tx.GetRuleset()
	if err != nil {
		return err
	}
	for _, rule := range ruleset {
		linkedOutputIds = append(linkedOutputIds, rule.RequiredOutputIds(tx)...)
	}

	// Gets outputs that are linked to by an input in the transaction.
	linkedOutputRes, err := bc.db.GetOutputs(linkedOutputIds)
	if err != nil {
		return err
	}

	acceptedOutputs := make(map[string]Output)
	undecidedOutputs := make(map[string]Output)

	for _, outputRes := range linkedOutputRes {
		output, err := fromDBOutput(outputRes.Output)
		if err != nil {
			return err
		}
		switch BlockState(outputRes.Block.State) {
		case BLOCK_STATE_UNDECIDED:
			undecidedOutputs[hashOutput(output).String()] = output
		case BLOCK_STATE_ACCEPTED:
			acceptedOutputs[hashOutput(output).String()] = output
		}
	}

	// Look for outputs that only exist in undecided or rejected blocks
	consumableOutputIds := make([][]byte, 0)
	undecidedOutputIds := make([][]byte, 0)
	rejectedOutputIds := make([][]byte, 0) // rejected or missing
	for _, outputId := range linkedOutputIds {
		if _, ok := acceptedOutputs[string(outputId)]; !ok {
			if _, undecidedOk := undecidedOutputs[string(outputId)]; undecidedOk {
				undecidedOutputIds = append(undecidedOutputIds, outputId)
			} else {
				rejectedOutputIds = append(rejectedOutputIds, outputId)
			}
		} else {
			if acceptedOutputs[string(outputId)].IsConsumable() {
				consumableOutputIds = append(consumableOutputIds, outputId)
			}
		}
	}

	if len(rejectedOutputIds) > 0 { // rejected or missing
		return &MissingOutputsError{OutputIds: rejectedOutputIds}
	} else if len(undecidedOutputs) > 0 {
		return &UndecidedOutputsError{OutputIds: undecidedOutputIds}
	}

	// For all consumable outputs, get all inputs that link to that output
	spentInputRes, err := bc.db.GetInputsByOutput(consumableOutputIds)
	if err != nil {
		return err
	}

	spentInputs := make(map[string][]Input)
	for _, inputRes := range spentInputRes {
		if BlockState(inputRes.Block.State) != BLOCK_STATE_REJECTED {
			input, err := fromDBInput(inputRes.Input)
			if err != nil {
				return err
			}
			outputId := input.OutputHash().String()
			if _, ok := spentInputs[outputId]; ok {
				spentInputs[outputId] = append(spentInputs[outputId], input)
			} else {
				spentInputs[outputId] = []Input{input}
			}
		}
	}

	return tx.Validate(acceptedOutputs, spentInputs)
}

// Proxy to db to delete transactions from backlog.
func (bc *Blockchain) DeleteTransactions(txs []*Transaction) error {
	dbTxs := make([]*meddb.Transaction, len(txs))
	for i, tx := range txs {
		dbTxs[i] = tx.toDBTransaction()
	}
	return bc.db.DeleteTransactions(dbTxs)
}

// Builds block from given transactions.
// DOES NOT VALIDATE TRANSACTIONS. That must be done before.
func (bc *Blockchain) BuildBlock(txs []*Transaction) (*Block, error) {
	if len(txs) == 0 {
		// TODO: Raise error here, should never be called with zero transactions
		return nil, nil
	}

	// TODO: Lock
	voters := make([][]byte, len(bc.federation))
	for i, node := range bc.federation {
		voters[i] = node.PubKey
	}

	// Create block out of transactions
	b := &Block{
		Transactions: txs,
		CreatedAt:    big.NewInt(common.Now()),
		Creator:      bc.me.PubKey,
		Voters:       voters,
	}

	// Sign block
	var err error
	b.Sig, err = crypto.Sign(b.Hash().Bytes(), bc.me.PrivKey)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Writes block to block table.
// Assumes that block and all of its transactions have been verified.
// Also, assumes that the block has been signed by this node.
func (bc *Blockchain) WriteBlock(b *Block) error {
	return bc.db.WriteBlock(b.toDBBlock())
}

// Returns blocks from blocks table for the given blockIds
func (bc *Blockchain) GetBlocks(blockIds []Hash) ([]*Block, error) {
	ids := make([][]byte, len(blockIds))
	for i, blockId := range blockIds {
		ids[i] = blockId.Bytes()
	}
	dbBs, err := bc.db.GetBlocks(ids)
	if err != nil {
		return nil, err
	}
	return fromDBBlocks(dbBs), nil
}

// Returns `limit` blocks from blocks table starting at given timestamp, sorted by increasing
// CreatedAt.
func (bc *Blockchain) GetOldestBlocks(after int64, limit int) ([]*Block, error) {
	dbBs, err := bc.db.GetOldestBlocks(after, limit)
	if err != nil {
		return nil, err
	}
	return fromDBBlocks(dbBs), nil
}

// Builds and signs vote for particular block, given previous block.
func (bc *Blockchain) BuildVote(blockId, prevBlockId Hash, value bool) (*Vote, error) {
	v := &Vote{
		Voter:     bc.me.PubKey,
		VotedAt:   big.NewInt(common.Now()),
		PrevBlock: prevBlockId,
		NextBlock: blockId,
		Value:     value,
	}

	// Sign vote
	var err error
	v.Sig, err = crypto.Sign(v.Hash().Bytes(), bc.me.PrivKey)
	if err != nil {
		return nil, err
	}

	return v, nil
}

// Writes vote to vote table.
// Assumes vote is already signed.
func (bc *Blockchain) WriteVote(v *Vote) error {
	if err := bc.db.WriteVote(v.toDBVote()); err != nil {
		return err
	}
	return nil
}

// Returns all of the votes with the timestamp of the most recent vote for this node.
func (bc *Blockchain) GetRecentVotes() ([]*Vote, error) {
	dbVs, err := bc.db.GetRecentVotes(bc.me.PubKey, 2)
	if err != nil {
		return nil, err
	}

	if len(dbVs) == 2 && dbVs[0].VotedAt.Cmp(dbVs[1].VotedAt) == 0 {
		// This rarely happens and that first call was a small optimization to only have to make
		// one query in most cases.
		dbVs, err = bc.db.GetVotes(bc.me.PubKey, dbVs[0].VotedAt.Int64())
		if err != nil {
			return nil, err
		}
	} else if len(dbVs) > 0 {
		// Only return the newest one.
		dbVs = dbVs[:1]
	}

	return fromDBVotes(dbVs), nil
}

// -------
// Helpers
// -------

// Returns a random node to assign a transaction to that is not this node.
func (bc *Blockchain) randomAssignee(seed int64) *Node {
	rand.Seed(seed)
	return bc.federation[rand.Intn(len(bc.federation))]
}
