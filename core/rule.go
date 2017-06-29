package core

type Rule interface {
	Validate(*Transaction, []*Output) error
}
