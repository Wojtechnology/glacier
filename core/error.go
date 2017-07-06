package core

import "fmt"

type MissingOutputsError struct {
	OutputIds [][]byte
}

func (e *MissingOutputsError) Error() string {
	return fmt.Sprintf("Outputs missing: %v", e.OutputIds)
}

type UndecidedOutputsError struct {
	OutputIds [][]byte
}

func (e *UndecidedOutputsError) Error() string {
	return fmt.Sprintf("Outputs undecided: %v", e.OutputIds)
}
