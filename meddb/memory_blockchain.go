package meddb

import "sync"

// In-memory blockchain db mainly meant for testing
type MemoryBlockchainDB struct {
	backlogTable map[string]*Transaction
	backlogLock  sync.RWMutex
}

// ----------------------
// MemoryBlockchainDB API
// ----------------------

func NewMemoryBlockchainDB() (*MemoryBlockchainDB, error) {
	return &MemoryBlockchainDB{
		backlogTable: make(map[string]*Transaction),
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
