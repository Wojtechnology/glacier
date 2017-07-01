package meddb

import "math/big"

type BlockchainDB interface {
	// First time setup to create required tables and indices
	SetupTables() error

	// Writes transaction to backlog table
	WriteTransaction(*Transaction) error
	// Returns transactions currently assigned to given node from backlog table
	GetAssignedTransactions([]byte) ([]*Transaction, error)
	// Returns transactions older than given time (no order) from backlog table
	GetStaleTransactions(int64) ([]*Transaction, error)
	// Deletes given transactions from backlog table
	DeleteTransactions([]*Transaction) error

	// Writes block to block table
	WriteBlock(*Block) error
	// Returns blocks from block table by block ids
	GetBlocks([][]byte) ([]*Block, error)
	// Returns k oldest blocks from block table starting at given timestamp sorted by increasing
	// CreatedAt timestamp.
	GetOldestBlocks(int64, int) ([]*Block, error)
	// Returns outputs for given output ids
	GetOutputs([][]byte) ([]*OutputRes, error)

	// Writes vote to vote table
	WriteVote(*Vote) error
	// Returns all votes for given public key from votes table with the given VotedAt
	GetVotes([]byte, int64) ([]*Vote, error)
	// Returns k most recent votes for given public key from votes table sorted by decreasing
	// VotedAt timestamp.
	GetRecentVotes([]byte, int) ([]*Vote, error)
}

type Transaction struct {
	Hash       []byte
	AssignedTo []byte // Public key of node this transaction is assigned to
	AssignedAt *big.Int
	TableName  []byte
	RowId      []byte
	Cols       map[string]*Cell
	Outputs    []*Output
	Inputs     []*Input
}

type Output struct {
	Hash []byte
	Type int
	Data []byte
}

type Input struct {
	Type       int
	OutputHash []byte
	Data       []byte
}

type Block struct {
	Hash         []byte
	Transactions []*Transaction
	CreatedAt    *big.Int
	Creator      []byte
	Voters       [][]byte
}

type Vote struct {
	Hash      []byte
	Voter     []byte
	VotedAt   *big.Int
	PrevBlock []byte
	NextBlock []byte // Block we are voting on
	Value     bool
}

// Structure used to return the result of the GetOutputs endpoint.
type OutputRes struct {
	Block  *Block
	Output *Output
}

func (tx *Transaction) Clone() *Transaction {
	var (
		lastAssigned *big.Int         = nil
		cols         map[string]*Cell = nil
		outputs      []*Output        = nil
		inputs       []*Input         = nil
	)

	if tx.AssignedAt != nil {
		lastAssigned = big.NewInt(tx.AssignedAt.Int64())
	}

	if tx.Cols != nil {
		cols = make(map[string]*Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = cell.Clone()
		}
	}

	if tx.Outputs != nil {
		outputs = make([]*Output, len(tx.Outputs))
		for i, output := range tx.Outputs {
			outputs[i] = output.Clone()
		}
	}

	if tx.Inputs != nil {
		inputs = make([]*Input, len(tx.Inputs))
		for i, input := range tx.Inputs {
			inputs[i] = input.Clone()
		}
	}

	return &Transaction{
		Hash:       tx.Hash,
		AssignedTo: tx.AssignedTo,
		AssignedAt: lastAssigned,
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
		Outputs:    outputs,
		Inputs:     inputs,
	}
}

func (o *Output) Clone() *Output {
	return &Output{
		Hash: o.Hash,
		Type: o.Type,
		Data: o.Data,
	}
}

func (in *Input) Clone() *Input {
	return &Input{
		Type:       in.Type,
		OutputHash: in.OutputHash,
		Data:       in.Data,
	}
}

func (b *Block) Clone() *Block {
	var createdAt *big.Int = nil
	if b.CreatedAt != nil {
		createdAt = big.NewInt(b.CreatedAt.Int64())
	}

	return &Block{
		Hash:         b.Hash,
		Transactions: b.Transactions,
		CreatedAt:    createdAt,
		Creator:      b.Creator,
		Voters:       b.Voters,
	}
}

func (v *Vote) Clone() *Vote {
	var votedAt *big.Int = nil
	if v.VotedAt != nil {
		votedAt = big.NewInt(v.VotedAt.Int64())
	}

	return &Vote{
		Hash:      v.Hash,
		Voter:     v.Voter,
		VotedAt:   votedAt,
		PrevBlock: v.PrevBlock,
		NextBlock: v.NextBlock,
		Value:     v.Value,
	}
}
