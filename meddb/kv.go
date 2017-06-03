package meddb

import "sync"

// -----------------
// Deprecated DB API
// -----------------

type Database interface {
	Get([]byte) ([]byte, error)
	Put([]byte, []byte) error
	Contains([]byte) (bool, error)
	Commit() error
	Delete([]byte) error
}

type MemoryDatabase struct {
	db   map[string][]byte
	lock sync.RWMutex
}

func NewMemoryDatabase() (*MemoryDatabase, error) {
	return &MemoryDatabase{db: make(map[string][]byte)}, nil
}

// Puts value into database at given key
func (db *MemoryDatabase) Put(key []byte, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	copiedValue := make([]byte, len(value))
	copy(copiedValue, value)

	db.db[string(key)] = copiedValue
	return nil
}

// Gets value from database with given key
// If value dues not exist, returns error and nil
func (db *MemoryDatabase) Get(key []byte) ([]byte, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if value, ok := db.db[string(key)]; ok {
		return value, nil
	}
	return nil, &NotFoundError{Key: key}
}

// Returns whether the database contains the key
func (db *MemoryDatabase) Contains(key []byte) (bool, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	_, ok := db.db[string(key)]
	return ok, nil
}

func (db *MemoryDatabase) Commit() error {
	return nil
}

// Delete the key from the database. Returns NotFoundError if the key does not exist.
func (db *MemoryDatabase) Delete(key []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if _, ok := db.db[string(key)]; !ok {
		return &NotFoundError{Key: key}
	}
	delete(db.db, string(key))
	return nil
}
