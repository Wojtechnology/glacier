package meddb

import (
	"sync"

	r "gopkg.in/gorethink/gorethink.v3"
)

type RethinkBigtable struct {
	session *r.Session
	lock    sync.RWMutex
}

func NewRethinkBigtable(addresses []string) (*RethinkBigtable, error) {
	session, err := r.Connect(r.ConnectOpts{
		Addresses: addresses,
	})
	if err != nil {
		return nil, err
	}
	t := &RethinkBigtable{session: session}
	return t, nil
}

func (t *RethinkBigtable) Put(tableName, rowId []byte, cells []*Cell) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	return nil
}

func (t *RethinkBigtable) Get(tableName, rowId []byte, cells []*Cell) ([]*Cell, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	return nil, nil
}

func (t *RethinkBigtable) CreateTable(tableName []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	return nil
}
