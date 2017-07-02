package meddb

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"

	r "gopkg.in/gorethink/gorethink.v3"
)

const (
	rethinkBacklogName = "backlog"
	rethinkBlockName   = "block"
	rethinkVoteName    = "vote"
)

type RethinkBlockchainDB struct {
	session *r.Session
	// TODO: Possibly remove lock, probably not needed
	lock     sync.RWMutex
	database string
}

type rethinkPartialCell struct {
	Data  []byte `gorethink:"data"`
	VerId []byte `gorethink:"ver_id"`
}

type rethinkTransaction struct {
	Hash       []byte                         `gorethink:"id"`
	AssignedTo []byte                         `gorethink:"assigned_to"`
	AssignedAt []byte                         `gorethink:"assigned_at"`
	Type       int                            `gorethink:"type"`
	TableName  []byte                         `gorethink:"table_name"`
	RowId      []byte                         `gorethink:"row_id"`
	Cols       map[string]*rethinkPartialCell `gorethink:"cols"`
	Outputs    []*rethinkOutput               `gorethink:"outputs"`
	Inputs     []*rethinkInput                `gorethink:"inputs"`
}

type rethinkOutput struct {
	Hash []byte `gorethink:"id"`
	Type int    `gorethink:"type"`
	Data []byte `gorethink:"data"`
}

type rethinkInput struct {
	Type       int    `gorethink:"type"`
	OutputHash []byte `gorethink:"output_hash"`
	Data       []byte `gorethink:"data"`
}

type rethinkBlock struct {
	Hash         []byte                `gorethink:"id"`
	Transactions []*rethinkTransaction `gorethink:"transactions"`
	CreatedAt    []byte                `gorethink:"created_at"`
	Creator      []byte                `gorethink:"creator"`
	Voters       [][]byte              `gorethink:"voters"`
	State        int                   `gorethink:"state"`
}

type rethinkVote struct {
	Hash      []byte `gorethink:"id"`
	Voter     []byte `gorethink:"voter"`
	VotedAt   []byte `gorethink:"voted_at"`
	PrevBlock []byte `gorethink:"prev_block"`
	NextBlock []byte `gorethink:"next_block"`
	Value     bool   `gorethink:"value"`
}

// ----------------------
// MemoryBlockchainDB API
// ----------------------

func NewRethinkBlockchainDB(addresses []string, database string) (*RethinkBlockchainDB, error) {
	session, err := r.Connect(r.ConnectOpts{
		Addresses: addresses,
	})
	if err != nil {
		return nil, err
	}
	t := &RethinkBlockchainDB{session: session, database: database}
	return t, nil
}

func (db *RethinkBlockchainDB) SetupTables() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	_, err := r.DBCreate(db.database).RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = r.DB(db.database).TableCreate(rethinkBacklogName).RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = r.DB(db.database).TableCreate(rethinkBlockName).RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = r.DB(db.database).TableCreate(rethinkVoteName).RunWrite(db.session)
	if err != nil {
		return err
	}
	err = db.setupBacklogIndices()
	if err != nil {
		return err
	}
	err = db.setupBlockIndices()
	if err != nil {
		return err
	}
	err = db.setupVoteIndices()
	if err != nil {
		return err
	}
	_, err = db.backlogTable().IndexWait().Run(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) setupBacklogIndices() error {
	_, err := db.backlogTable().IndexCreate("assigned_to").RunWrite(db.session)
	if err != nil {
		return err
	}
	return nil
}

func (db *RethinkBlockchainDB) setupBlockIndices() error {
	_, err := db.blockTable().IndexCreate("created_at").RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = db.blockTable().IndexCreateFunc("outputs", func(block r.Term) interface{} {
		return block.Field("transactions").ConcatMap(func(tx r.Term) interface{} {
			return tx.Field("outputs").Map(func(output r.Term) interface{} {
				return output.Field("id")
			})
		})
	}, r.IndexCreateOpts{Multi: true}).RunWrite(db.session)
	if err != nil {
		return err
	}
	_, err = db.blockTable().IndexCreateFunc("input_outputs", func(block r.Term) interface{} {
		return block.Field("transactions").ConcatMap(func(tx r.Term) interface{} {
			return tx.Field("inputs").Map(func(input r.Term) interface{} {
				return input.Field("output_hash")
			})
		})
	}, r.IndexCreateOpts{Multi: true}).RunWrite(db.session)
	if err != nil {
		return err
	}
	return nil
}

func (db *RethinkBlockchainDB) setupVoteIndices() error {
	_, err := db.voteTable().IndexCreateFunc("voter__voted_at", func(row r.Term) interface{} {
		return []interface{}{row.Field("voter"), row.Field("voted_at")}
	}).RunWrite(db.session)
	if err != nil {
		return err
	}
	return nil
}

func (db *RethinkBlockchainDB) WriteTransaction(tx *Transaction) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	rethinkTx := newRethinkTransaction(tx)
	_, err := db.backlogTable().Insert(rethinkTx, r.InsertOpts{
		Conflict: "replace",
	}).RunWrite(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) GetAssignedTransactions(pubKey []byte) ([]*Transaction, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	res, err := db.backlogTable().GetAllByIndex("assigned_to", pubKey).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkTransaction
	if err := res.All(&rows); err != nil {
		return nil, err
	}
	return fromRethinkTransactions(rows), nil
}

func (db *RethinkBlockchainDB) GetStaleTransactions(before int64) ([]*Transaction, error) {

	db.lock.Lock()
	defer db.lock.Unlock()

	res, err := db.backlogTable().Filter(
		r.And(
			// Not sure why Not().Eq(nil) doesn't work, but this does so going to leave it
			r.Row.Field("assigned_at").Ge(int64ToBytes(0)),
			r.Row.Field("assigned_at").Le(int64ToBytes(before)),
		),
	).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkTransaction
	if err := res.All(&rows); err != nil {
		return nil, err
	}
	return fromRethinkTransactions(rows), nil
}

func (db *RethinkBlockchainDB) DeleteTransactions(txs []*Transaction) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	ids := make([]interface{}, len(txs))
	for i, tx := range txs {
		ids[i] = tx.Hash
	}

	_, err := db.backlogTable().GetAll(ids...).Delete().RunWrite(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) WriteBlock(b *Block) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	rethinkB := newRethinkBlock(b)
	_, err := db.blockTable().Insert(rethinkB, r.InsertOpts{
		Conflict: "replace",
	}).RunWrite(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) GetBlocks(blockIds [][]byte) ([]*Block, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	ids := make([]interface{}, len(blockIds)) // Why do I have to do this?
	for i, blockId := range blockIds {
		ids[i] = blockId
	}

	res, err := db.blockTable().GetAll(ids...).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkBlock
	if err := res.All(&rows); err != nil {
		return nil, err
	}

	if len(rows) != len(blockIds) {
		foundIds := make([][]byte, len(rows))
		for i, row := range rows {
			foundIds[i] = row.Hash
		}
		return nil, errors.New(fmt.Sprintf("Some blocks not found. Found: %v, All %v\n",
			foundIds, blockIds))
	}

	return fromRethinkBlocks(rows), nil
}

func (db *RethinkBlockchainDB) GetOldestBlocks(start int64, limit int) ([]*Block, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	res, err := db.blockTable().Between(int64ToBytes(start), r.MaxVal, r.BetweenOpts{
		Index: "created_at",
	}).OrderBy(r.OrderByOpts{
		Index: "created_at",
	}).Limit(limit).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkBlock
	if err := res.All(&rows); err != nil {
		return nil, err
	}

	return fromRethinkBlocks(rows), nil
}

type rethinkOutputRes struct {
	Block  *rethinkBlock  `gorethink:"block"`
	Output *rethinkOutput `gorethink:"output"`
}

func (db *RethinkBlockchainDB) GetOutputs(outputIds [][]byte) ([]*OutputRes, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	ids := make([]interface{}, len(outputIds)) // Why do I have to do this?
	for i, outputId := range outputIds {
		ids[i] = outputId
	}

	res, err := db.blockTable().GetAllByIndex(
		"outputs", ids...,
	).Distinct().ConcatMap(func(block r.Term) interface{} {
		return block.Field("transactions").ConcatMap(func(tx r.Term) interface{} {
			return tx.Field("outputs").Map(func(output r.Term) interface{} {
				return map[string]interface{}{
					"block":  block.Without("transactions"),
					"output": output,
				}
			})
		})
	}).Filter(func(row r.Term) interface{} {
		return r.Expr(ids).Contains(row.Field("output").Field("id"))
	}).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkOutputRes
	if err := res.All(&rows); err != nil {
		return nil, err
	}

	return fromRethinkOutputRes(rows), nil
}

type rethinkInputRes struct {
	Block *rethinkBlock `gorethink:"block"`
	Input *rethinkInput `gorethink:"input"`
}

func (db *RethinkBlockchainDB) GetInputsByOutput(outputIds [][]byte) ([]*InputRes, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	ids := make([]interface{}, len(outputIds)) // Why do I have to do this?
	for i, outputId := range outputIds {
		ids[i] = outputId
	}

	res, err := db.blockTable().GetAllByIndex(
		"input_outputs", ids...,
	).Distinct().ConcatMap(func(block r.Term) interface{} {
		return block.Field("transactions").ConcatMap(func(tx r.Term) interface{} {
			return tx.Field("inputs").Map(func(input r.Term) interface{} {
				return map[string]interface{}{
					"block": block.Without("transactions"),
					"input": input,
				}
			})
		})
	}).Filter(func(row r.Term) interface{} {
		return r.Expr(ids).Contains(row.Field("input").Field("output_hash"))
	}).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkInputRes
	if err := res.All(&rows); err != nil {
		return nil, err
	}

	return fromRethinkInputRes(rows), nil
}

func (db *RethinkBlockchainDB) WriteVote(v *Vote) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	rethinkV := newRethinkVote(v)
	_, err := db.voteTable().Insert(rethinkV, r.InsertOpts{
		Conflict: "replace",
	}).RunWrite(db.session)
	if err != nil {
		return err
	}

	return nil
}

func (db *RethinkBlockchainDB) GetVotes(pubKey []byte, votedAt int64) ([]*Vote, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	res, err := db.voteTable().GetAllByIndex("voter__voted_at",
		[]interface{}{pubKey, int64ToBytes(votedAt)},
	).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkVote
	if err := res.All(&rows); err != nil {
		return nil, err
	}

	return fromRethinkVotes(rows), nil
}

func (db *RethinkBlockchainDB) GetRecentVotes(pubKey []byte, limit int) ([]*Vote, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	res, err := db.voteTable().Between(
		[]interface{}{pubKey, int64ToBytes(math.MinInt64)},
		[]interface{}{pubKey, int64ToBytes(math.MaxInt64)},
		r.BetweenOpts{Index: "voter__voted_at"},
	).OrderBy(r.OrderByOpts{Index: r.Desc("voter__voted_at")}).Limit(limit).Run(db.session)
	if err != nil {
		return nil, err
	}

	var rows []*rethinkVote
	if err := res.All(&rows); err != nil {
		return nil, err
	}

	return fromRethinkVotes(rows), nil
}

// -------
// Helpers
// -------

func (db *RethinkBlockchainDB) backlogTable() r.Term {
	return r.DB(db.database).Table(rethinkBacklogName)
}

func (db *RethinkBlockchainDB) blockTable() r.Term {
	return r.DB(db.database).Table(rethinkBlockName)
}

func (db *RethinkBlockchainDB) voteTable() r.Term {
	return r.DB(db.database).Table(rethinkVoteName)
}

func newRethinkPartialCell(cell *Cell) *rethinkPartialCell {
	var verId []byte = nil
	if cell.VerId != nil {
		verId = int64ToBytes(cell.VerId.Int64())
	}
	return &rethinkPartialCell{
		Data:  cell.Data,
		VerId: verId,
	}
}

func fromRethinkPartialCell(cell *rethinkPartialCell) *Cell {
	var verId *big.Int = nil
	if cell.VerId != nil {
		verId = big.NewInt(bytesToInt64(cell.VerId))
	}
	return &Cell{
		Data:  cell.Data,
		VerId: verId,
	}
}

func newRethinkTransaction(tx *Transaction) *rethinkTransaction {
	var (
		assignedAt []byte                         = nil
		cols       map[string]*rethinkPartialCell = nil
		outputs    []*rethinkOutput               = nil
		inputs     []*rethinkInput                = nil
	)

	if tx.AssignedAt != nil {
		assignedAt = int64ToBytes(tx.AssignedAt.Int64())
	}

	if tx.Cols != nil {
		cols = make(map[string]*rethinkPartialCell)
		for colId, cell := range tx.Cols {
			cols[colId] = newRethinkPartialCell(cell)
		}
	}

	if tx.Outputs != nil {
		outputs = make([]*rethinkOutput, len(tx.Outputs))
		for i, output := range tx.Outputs {
			outputs[i] = newRethinkOutput(output)
		}
	}

	if tx.Inputs != nil {
		inputs = make([]*rethinkInput, len(tx.Inputs))
		for i, input := range tx.Inputs {
			inputs[i] = newRethinkInput(input)
		}
	}

	return &rethinkTransaction{
		Hash:       tx.Hash,
		AssignedTo: tx.AssignedTo,
		AssignedAt: assignedAt,
		Type:       tx.Type,
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
		Outputs:    outputs,
		Inputs:     inputs,
	}
}

func fromRethinkTransaction(tx *rethinkTransaction) *Transaction {
	var (
		assignedAt *big.Int         = nil
		cols       map[string]*Cell = nil
		outputs    []*Output        = nil
		inputs     []*Input         = nil
	)

	if tx.AssignedAt != nil && len(tx.AssignedAt) == 8 {
		assignedAt = big.NewInt(bytesToInt64(tx.AssignedAt))
	}

	if tx.Cols != nil {
		cols = make(map[string]*Cell)
		for colId, cell := range tx.Cols {
			cols[colId] = fromRethinkPartialCell(cell)
		}
	}

	if tx.Outputs != nil {
		outputs = make([]*Output, len(tx.Outputs))
		for i, output := range tx.Outputs {
			outputs[i] = fromRethinkOutput(output)
		}
	}

	if tx.Inputs != nil {
		inputs = make([]*Input, len(tx.Inputs))
		for i, input := range tx.Inputs {
			inputs[i] = fromRethinkInput(input)
		}
	}

	return &Transaction{
		Hash:       tx.Hash,
		AssignedTo: tx.AssignedTo,
		AssignedAt: assignedAt,
		Type:       tx.Type,
		TableName:  tx.TableName,
		RowId:      tx.RowId,
		Cols:       cols,
		Outputs:    outputs,
		Inputs:     inputs,
	}

}

func fromRethinkTransactions(rethinkTxs []*rethinkTransaction) []*Transaction {
	txs := make([]*Transaction, len(rethinkTxs))
	for i, row := range rethinkTxs {
		txs[i] = fromRethinkTransaction(row)
	}
	return txs
}

func newRethinkOutput(o *Output) *rethinkOutput {
	return &rethinkOutput{
		Hash: o.Hash,
		Type: o.Type,
		Data: o.Data,
	}
}

func newRethinkInput(in *Input) *rethinkInput {
	return &rethinkInput{
		Type:       in.Type,
		OutputHash: in.OutputHash,
		Data:       in.Data,
	}
}

func fromRethinkOutput(o *rethinkOutput) *Output {
	return &Output{
		Hash: o.Hash,
		Type: o.Type,
		Data: o.Data,
	}
}

func fromRethinkInput(in *rethinkInput) *Input {
	return &Input{
		Type:       in.Type,
		OutputHash: in.OutputHash,
		Data:       in.Data,
	}
}

func newRethinkBlock(b *Block) *rethinkBlock {
	var createdAt []byte = nil
	if b.CreatedAt != nil {
		createdAt = int64ToBytes(b.CreatedAt.Int64())
	}

	var txs []*rethinkTransaction = nil
	if b.Transactions != nil {
		txs = make([]*rethinkTransaction, len(b.Transactions))
		for i, tx := range b.Transactions {
			txs[i] = newRethinkTransaction(tx)
		}
	}

	return &rethinkBlock{
		Hash:         b.Hash,
		Transactions: txs,
		CreatedAt:    createdAt,
		Creator:      b.Creator,
		Voters:       b.Voters,
		State:        b.State,
	}
}

func fromRethinkBlock(b *rethinkBlock) *Block {
	var createdAt *big.Int = nil
	if b.CreatedAt != nil && len(b.CreatedAt) == 8 {
		createdAt = big.NewInt(bytesToInt64(b.CreatedAt))
	}

	var txs []*Transaction = nil
	if b.Transactions != nil {
		txs = make([]*Transaction, len(b.Transactions))
		for i, tx := range b.Transactions {
			txs[i] = fromRethinkTransaction(tx)
		}
	}

	return &Block{
		Hash:         b.Hash,
		Transactions: txs,
		CreatedAt:    createdAt,
		Creator:      b.Creator,
		Voters:       b.Voters,
		State:        b.State,
	}
}

func fromRethinkBlocks(rethinkBs []*rethinkBlock) []*Block {
	bs := make([]*Block, len(rethinkBs))
	for i, row := range rethinkBs {
		bs[i] = fromRethinkBlock(row)
	}
	return bs
}

func newRethinkVote(v *Vote) *rethinkVote {
	var votedAt []byte = nil
	if v.VotedAt != nil {
		votedAt = int64ToBytes(v.VotedAt.Int64())
	}

	return &rethinkVote{
		Hash:      v.Hash,
		Voter:     v.Voter,
		VotedAt:   votedAt,
		PrevBlock: v.PrevBlock,
		NextBlock: v.NextBlock,
		Value:     v.Value,
	}
}

func fromRethinkVote(v *rethinkVote) *Vote {
	var votedAt *big.Int = nil
	if v.VotedAt != nil && len(v.VotedAt) == 8 {
		votedAt = big.NewInt(bytesToInt64(v.VotedAt))
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

func fromRethinkVotes(rethinkVs []*rethinkVote) []*Vote {
	vs := make([]*Vote, len(rethinkVs))
	for i, row := range rethinkVs {
		vs[i] = fromRethinkVote(row)
	}
	return vs
}

func fromRethinkOutputRes(rows []*rethinkOutputRes) []*OutputRes {
	newRows := make([]*OutputRes, len(rows))
	for i, row := range rows {
		newRows[i] = &OutputRes{
			Block:  fromRethinkBlock(row.Block),
			Output: fromRethinkOutput(row.Output),
		}
	}
	return newRows
}

func fromRethinkInputRes(rows []*rethinkInputRes) []*InputRes {
	newRows := make([]*InputRes, len(rows))
	for i, row := range rows {
		newRows[i] = &InputRes{
			Block: fromRethinkBlock(row.Block),
			Input: fromRethinkInput(row.Input),
		}
	}
	return newRows
}
