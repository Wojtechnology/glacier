package core

type Rule interface {
	IsReusable() bool
	GetRequiredOutputs(*Blockchain) ([]*Output, error)
	Validate(*Transaction, []*Output) error
}

type ReusableRule struct{}

func (r *ReusableRule) isReusable() bool {
	return true
}

type ConsumableRule struct{}

func (r *ConsumableRule) isReusable() bool {
	return false
}
