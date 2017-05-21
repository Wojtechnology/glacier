package meddb

import (
	"fmt"
	"sync"

	"github.com/gocql/gocql"
)

type CassandraBigtable struct {
	session *gocql.Session
	lock    sync.RWMutex
}

func NewCassandraBigtable(addresses []string, keyspace string) (*CassandraBigtable, error) {
	cluster := gocql.NewCluster(addresses...)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &CassandraBigtable{session: session}, nil
}

func (t *CassandraBigtable) Put(tableName, rowId []byte, cells []*Cell) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	return nil
}

func (t *CassandraBigtable) Get(tableName, rowId []byte, cells []*Cell) ([]*Cell, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	return nil, nil
}

const casCreateBigtable = `
	CREATE TABLE %s (
		row_id TEXT,
        ver_id INT,
        col_id TEXT,
        data TEXT,
        PRIMARY KEY (row_id, ver_id, col_id)
	)
`

func (t *CassandraBigtable) CreateTable(tableName []byte, colNames [][]byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	err := t.session.Query(fmt.Sprintf(casCreateBigtable, string(tableName))).Exec()
	if err != nil {
		return err
	}
	return nil
}
