package meddb

import "fmt"

type NotFoundError struct {
	Key []byte
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("Key \"%s\" does not exist in db\n", e.Key)
}

type ColIdAlreadyExists struct {
	ColId []byte
}

func (e *ColIdAlreadyExists) Error() string {
	return fmt.Sprintf("ColId \"%s\" already exists", e.ColId)
}

type TableNotFoundError struct {
	TableName []byte
}

func (e *TableNotFoundError) Error() string {
	return fmt.Sprintf("Table \"%s\" not found", e.TableName)
}

type RowNotFoundError struct {
	RowId []byte
}

func (e *RowNotFoundError) Error() string {
	return fmt.Sprintf("Row \"%s\" not found", e.RowId)
}
