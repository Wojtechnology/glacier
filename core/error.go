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

type RuleErrors struct {
	Errors []error
}

func (e *RuleErrors) Error() string {
	return fmt.Sprintf("Some errors occured when validating transaction: %v\n", e.Errors)
}
