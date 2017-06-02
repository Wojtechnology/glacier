package meddb

import "time"

type Bigtable interface {
	Put(tableName []byte, op *PutOp) error
	Get(tableName []byte, op *GetOp) (map[string][]*Cell, error)
	CreateTable(tableName []byte) error
	// TODO(wojtek): Delete
}

func curTimeMillis() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
}
