package core

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"

	"github.com/wojtechnology/glacier/meddb"
)

// Enum for row rule types
type RowRuleType int

const (
	ROW_RULE_ALL   RowRuleType = iota // ALL   = 1 - anyone
	ROW_RULE_OWNER                    // OWNER = 2 - only the first writer
)

// Enum for specifying which columns to read/write from/to table metadata table
type TableMetadataFlag int

const (
	TABLE_METADATA_ADMINS TableMetadataFlag = 1 << iota
	TABLE_METADATA_WRITERS
	TABLE_METADATA_ROW_RULES
	TABLE_METADATA_COL_RULES
	TABLE_METADATA_ALL TableMetadataFlag = 0
)

// Name of table metadata table
const TABLE_METADATA_TABLE = "table_metadata"

// Map from TableMetadataFlag to the column name
var TABLE_METADATA_MAP = map[TableMetadataFlag]string{
	TABLE_METADATA_ADMINS:    "admins",
	TABLE_METADATA_WRITERS:   "writers",
	TABLE_METADATA_ROW_RULES: "row_rules",
	TABLE_METADATA_COL_RULES: "col_rules",
}

// --------------------------
// Helpers for Table Metadata
// --------------------------

type RowRules struct {
	Type *big.Int
}

type ColRules struct {
	AllowedColIds [][]byte // List of col ids that are allows to exist in this table
}

type TableMetadata struct {
	TableName []byte    // Name of the table this metadata is for
	Admins    [][]byte  // List of public keys of admins of this table
	Writers   [][]byte  // List of public keys that can write to this table
	RowRules  *RowRules // Row level rules
	ColRules  *ColRules // Col level rules
}

// Writes non-null fields (specified by flag) of TableMetadata to bigtable
// Flag == 0 means to write all fields
func (tm *TableMetadata) Write(bt meddb.Bigtable, flag TableMetadataFlag) error {
	op := meddb.NewPutOp([]byte(tm.TableName))

	for metaFlag, colId := range TABLE_METADATA_MAP {
		if flag == TABLE_METADATA_ALL || flag&metaFlag != 0 {
			b, err := tm.getRlpAttribute(metaFlag)
			if err != nil {
				return err
			}
			if b != nil {
				op.AddCol([]byte(colId), b)
			}
		}
	}

	err := bt.Put([]byte(TABLE_METADATA_TABLE), op)
	if err != nil {
		return err
	}
	return nil
}

// Writes non-null fields (specified by flag) of TableMetadata from bigtable
// Flag == 0 means to read all fields
func (tm *TableMetadata) Read(bt meddb.Bigtable, flag TableMetadataFlag) error {
	colIds := make([][]byte, 0)
	for metaFlag, colId := range TABLE_METADATA_MAP {
		if flag == TABLE_METADATA_ALL || flag&metaFlag != 0 {
			colIds = append(colIds, []byte(colId))
		}
	}

	op := meddb.NewGetOpLimit([]byte(tm.TableName), colIds, 1)
	res, err := bt.Get([]byte(TABLE_METADATA_TABLE), op)
	if err != nil {
		return err
	}

	for metaFlag, colId := range TABLE_METADATA_MAP {
		if cells, ok := res[string(colId)]; ok && len(cells) > 0 {
			if err := tm.setRlpAttribute(metaFlag, cells[0].Data); err != nil {
				return err
			}
		}
	}

	return nil
}

// Returns rlp encoded attribute as byte string
func (tm *TableMetadata) getRlpAttribute(flag TableMetadataFlag) ([]byte, error) {
	var o interface{}
	switch flag {
	case TABLE_METADATA_ADMINS:
		o = tm.Admins
	case TABLE_METADATA_WRITERS:
		o = tm.Writers
	case TABLE_METADATA_ROW_RULES:
		o = tm.RowRules
	case TABLE_METADATA_COL_RULES:
		o = tm.ColRules
	default:
		return nil, errors.New(fmt.Sprintf("Invalid TableMetadataFlag: %d\n", flag))
	}

	if reflect.ValueOf(o).IsNil() {
		// Avoid writing nils
		return nil, nil
	}

	b, err := rlpEncode(o)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (tm *TableMetadata) setRlpAttribute(flag TableMetadataFlag, b []byte) error {
	var o interface{}
	switch flag {
	case TABLE_METADATA_ADMINS:
		o = &[][]byte{}
	case TABLE_METADATA_WRITERS:
		o = &[][]byte{}
	case TABLE_METADATA_ROW_RULES:
		o = new(RowRules)
	case TABLE_METADATA_COL_RULES:
		o = new(ColRules)
	default:
		return errors.New(fmt.Sprintf("Invalid TableMetadataFlag: %d\n", flag))
	}

	if err := rlpDecode(b, o); err != nil {
		return err
	}

	switch flag {
	case TABLE_METADATA_ADMINS:
		tm.Admins = *o.(*[][]byte)
	case TABLE_METADATA_WRITERS:
		tm.Writers = *o.(*[][]byte)
	case TABLE_METADATA_ROW_RULES:
		tm.RowRules = o.(*RowRules)
	case TABLE_METADATA_COL_RULES:
		tm.ColRules = o.(*ColRules)
		// Default case will never happen
	}
	return nil
}
