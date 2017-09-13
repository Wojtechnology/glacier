package core

import (
	"bytes"
	"errors"
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

// Temp method, has hardcoded stuff
func InitBlockchain(me *Node) (*Blockchain, error) {
	addresses := []string{"localhost"}
	database := "prod"

	// Init db that contains meddb
	db, err := meddb.NewRethinkBlockchainDB(addresses, database)
	if err != nil {
		return nil, err
	}

	// Init bigtable that contains cells
	bt, err := meddb.NewRethinkBigtable(addresses, database)
	if err != nil {
		return nil, err
	}

	bc := NewBlockchain(
		db,
		bt,
		me,
		[]*Node{me},
	)
	return bc, nil
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
	outputReqs := map[string]OutputRequirement{}
	for _, input := range tx.Inputs {
		// Linked outputs are required
		outputReqs[input.OutputHash().String()] = OUTPUT_REQUIREMENT_REQUIRED
	}

	ruleset, err := tx.GetRuleset()
	if err != nil {
		return err
	}
	for _, rule := range ruleset {
		ruleOutputReqs := rule.RequestedOutputIds(tx)
		for outputStrId, outputReq := range ruleOutputReqs {
			// We want the strictest requirement in the map
			if oldOutputReq, ok := outputReqs[outputStrId]; !ok || outputReq > oldOutputReq {
				outputReqs[outputStrId] = outputReq
			}
		}
	}

	i := 0
	outputIds := make([][]byte, len(outputReqs))
	for outputStrId, _ := range outputReqs {
		outputIds[i] = []byte(outputStrId)
		i++
	}

	// Gets all required outputs from database.
	outputResponses, err := bc.db.GetOutputs(outputIds)
	if err != nil {
		return err
	}

	// TODO: Replace with transaction level caching of hash
	txHash := tx.Hash().Bytes()

	// Get the state of all outputs.
	acceptedOutputs := make(map[string]Output)
	undecidedOutputs := make(map[string]Output)
	for _, outputRes := range outputResponses {
		// Want to ignore outputs from the same transaction
		if !bytes.Equal(txHash, outputRes.Transaction.Hash) {
			dbOutput := outputRes.Output
			output, err := NewOutput(OutputType(dbOutput.Type), dbOutput.Data)
			// TODO: Probably just ignore the output here.
			if err != nil {
				return err
			}
			switch BlockState(outputRes.Block.State) {
			case BLOCK_STATE_UNDECIDED:
				undecidedOutputs[HashOutput(output).String()] = output
			case BLOCK_STATE_ACCEPTED:
				acceptedOutputs[HashOutput(output).String()] = output
			}
		}
	}

	// Look at output requirements and make sure that they are met.
	// The strategy for this is optimisitic. I.E. if there exists an accepted and undecided version
	// of some output, it will take the accepted version.
	undecidedOutputIds := make([][]byte, 0)
	rejectedOutputIds := make([][]byte, 0) // rejected or missing
	for _, outputId := range outputIds {
		outputStr := string(outputId)
		if _, ok := acceptedOutputs[outputStr]; !ok {
			req := outputReqs[outputStr]
			if _, undecidedOk := undecidedOutputs[outputStr]; undecidedOk {
				// At least as strict as DECIDED
				if req >= OUTPUT_REQUIREMENT_DECIDED {
					undecidedOutputIds = append(undecidedOutputIds, outputId)
				}
			} else {
				// At least as strict as REQUIRED
				if req >= OUTPUT_REQUIREMENT_REQUIRED {
					rejectedOutputIds = append(rejectedOutputIds, outputId)
				}
			}
		}
	}

	if len(rejectedOutputIds) > 0 { // rejected or missing and were required
		return &MissingOutputsError{OutputIds: rejectedOutputIds}
	} else if len(undecidedOutputIds) > 0 { // in undecided block but was required to be decided
		return &UndecidedOutputsError{OutputIds: undecidedOutputIds}
	}

	// No currency yet, so spentInputs is empty
	return tx.Validate(acceptedOutputs, map[string][]Input{})
}

// Proxy to db to delete transactions from backlog.
func (bc *Blockchain) DeleteTransactions(txs []*Transaction) error {
	dbTxs := make([]*meddb.Transaction, len(txs))
	for i, tx := range txs {
		dbTxs[i] = tx.toDBTransaction()
	}
	return bc.db.DeleteTransactions(dbTxs)
}

// Returns meddb changefeed cursor for transactions assigned to this node.
func (bc *Blockchain) GetMyTransactionChangefeed() (*TransactionChangeCursor, error) {
	changefeed, err := bc.db.GetAssignedTransactionChangefeed(bc.me.PubKey)
	if err != nil {
		return nil, err
	}
	return &TransactionChangeCursor{changefeed: changefeed}, nil
}

// Creates the genesis block which contains one transaction with the message in GENESIS_MESSAGE.
// Generally this message should be some string that could not have been created before the date
// of blockchain genesis.
func (bc *Blockchain) BuildGenesis() (*Block, error) {
	genTx := &Transaction{
		Type: TransactionType(-1), // Reserved type should never be used for other transactions,
		// although not a big deal if it does.
		Cols: map[string]*Cell{
			"message": &Cell{Data: []byte(GENESIS_MESSAGE)},
		},
	}

	return bc.BuildBlock([]*Transaction{genTx})
}

// Builds block from given transactions.
// DOES NOT VALIDATE TRANSACTIONS. That must be done before.
func (bc *Blockchain) BuildBlock(txs []*Transaction) (*Block, error) {
	if len(txs) == 0 {
		// TODO: Raise error here, should never be called with zero transactions
		return nil, errors.New("Cannot build block with zero transactions")
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

// Validates block.
// Checks whether the signature of the block is valid.
// Checks whether the transactions within the block are valid.
func (bc *Blockchain) ValidateBlock(b *Block) error {
	// Check whether signature is valid
	pubKey, err := crypto.RetrievePublicKey(b.Hash().Bytes(), b.Sig)
	if err != nil {
		return err
	}

	if !bytes.Equal(pubKey, b.Creator) {
		return &BlockSignatureInvalidError{BlockId: b.Hash()}
	}

	// Check whether transactions are valid
	errs := make([]error, 0)
	for _, tx := range b.Transactions {
		err := bc.ValidateTransaction(tx)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return &TransactionErrors{BlockId: b.Hash(), Errors: errs}
	}

	return nil
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

func (bc *Blockchain) GetBlockChangefeed() (*BlockChangeCursor, error) {
	changefeed, err := bc.db.GetBlockChangefeed()
	if err != nil {
		return nil, err
	}
	return &BlockChangeCursor{changefeed: changefeed}, nil
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
