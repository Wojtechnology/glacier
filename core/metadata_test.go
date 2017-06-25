package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wojtechnology/glacier/meddb"
)

func TestTableMetadataReadWrite(t *testing.T) {
	bt, err := meddb.NewMemoryBigtable()
	assert.Nil(t, err)
	err = bt.CreateTable([]byte(TABLE_METADATA_TABLE))
	assert.Nil(t, err)

	tableName := []byte("Some table")
	meta := &TableMetadata{
		TableName: tableName,
		Admins:    [][]byte{[]byte("me")},
		Writers:   [][]byte{[]byte("me"), []byte("you")},
		RowRules:  &RowRules{Type: intToBigInt(int(ROW_RULE_ALL))},
		ColRules:  &ColRules{AllowedColIds: [][]byte{[]byte("stuff")}},
	}

	err = meta.Write(bt, TABLE_METADATA_ALL)
	assert.Nil(t, err)

	metaCopy := &TableMetadata{TableName: tableName}
	err = metaCopy.Read(bt, TABLE_METADATA_ALL)
	assert.Nil(t, err)

	assert.Equal(t, meta, metaCopy)
}

func TestTableMetadataReadWritePartial(t *testing.T) {
	bt, err := meddb.NewMemoryBigtable()
	assert.Nil(t, err)
	err = bt.CreateTable([]byte(TABLE_METADATA_TABLE))
	assert.Nil(t, err)

	tableName := []byte("Some table")
	flag := TABLE_METADATA_ADMINS | TABLE_METADATA_WRITERS | TABLE_METADATA_ROW_RULES
	meta := &TableMetadata{
		TableName: tableName,
		Admins:    [][]byte{[]byte("me")},
		ColRules:  &ColRules{AllowedColIds: [][]byte{[]byte("stuff")}},
	}

	err = meta.Write(bt, flag)
	assert.Nil(t, err)

	metaCopy := &TableMetadata{TableName: tableName}
	err = metaCopy.Read(bt, flag)
	assert.Nil(t, err)

	expected := &TableMetadata{
		TableName: tableName,
		Admins:    [][]byte{[]byte("me")},
	}

	assert.Equal(t, expected, metaCopy)
}
