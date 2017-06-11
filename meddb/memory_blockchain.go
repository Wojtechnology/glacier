package meddb

import (
	"bytes"
	"sync"
)

// In-memory blockchain db mainly meant for testing
type MemoryBlockchainDB struct {
	backlogTable map[string]*Transaction
	backlogLock  sync.RWMutex
	blockTable   map[string]*Block
	blockLock    sync.RWMutex
	voteTable    map[string]*Vote
	voteLock     sync.RWMutex
}

// ----------------------
// MemoryBlockchainDB API
// ----------------------

func NewMemoryBlockchainDB() (*MemoryBlockchainDB, error) {
	return &MemoryBlockchainDB{
		backlogTable: make(map[string]*Transaction),
		blockTable:   make(map[string]*Block),
		voteTable:    make(map[string]*Vote),
	}, nil
}

func (db *MemoryBlockchainDB) SetupTables() error {
	return nil
}

func (db *MemoryBlockchainDB) WriteTransaction(tx *Transaction) error {
	db.backlogLock.Lock()
	defer db.backlogLock.Unlock()

	db.backlogTable[string(tx.Hash)] = tx.Clone()
	return nil
}

// Note: This is not performant, do not use in prod
func (db *MemoryBlockchainDB) GetAssignedTransactions(pubKey []byte) ([]*Transaction, error) {
	db.backlogLock.Lock()
	defer db.backlogLock.Unlock()

	txs := make([]*Transaction, 0)
	for _, tx := range db.backlogTable {
		if bytes.Equal(tx.AssignedTo, pubKey) {
			txs = append(txs, tx.Clone())
		}
	}

	return txs, nil
}

func (db *MemoryBlockchainDB) DeleteTransactions(txs []*Transaction) error {
	db.backlogLock.Lock()
	defer db.backlogLock.Unlock()

	for _, tx := range txs {
		delete(db.backlogTable, string(tx.Hash))
	}

	return nil
}

func (db *MemoryBlockchainDB) WriteBlock(b *Block) error {
	db.blockLock.Lock()
	defer db.blockLock.Unlock()

	db.blockTable[string(b.Hash)] = b.Clone()
	return nil
}

func (db *MemoryBlockchainDB) WriteVote(v *Vote) error {
	db.voteLock.Lock()
	defer db.voteLock.Unlock()

	db.voteTable[string(v.Hash)] = v.Clone()
	return nil
}
