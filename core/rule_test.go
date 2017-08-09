package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidOutputTypesRule(t *testing.T) {
	tx := &Transaction{
		Outputs: []Output{
			&TableExistsOutput{},
			&ColAllowedOutput{},
		},
	}

	rule := &ValidOutputTypesRule{
		validTypes: map[OutputType]bool{
			OUTPUT_TYPE_TABLE_EXISTS: true,
			OUTPUT_TYPE_COL_ALLOWED:  true,
		},
	}

	assert.Nil(t, rule.Validate(tx, nil, nil))
}

func TestValidOutputTypesRuleInvalid(t *testing.T) {
	tx := &Transaction{
		Outputs: []Output{
			&TableExistsOutput{},
			&ColAllowedOutput{},
		},
	}

	rule := &ValidOutputTypesRule{
		validTypes: map[OutputType]bool{
			OUTPUT_TYPE_TABLE_EXISTS: true,
		},
	}

	assert.IsType(t, errors.New(""), rule.Validate(tx, nil, nil))
}

func TestHasTableExistsRule(t *testing.T) {
	tx := &Transaction{
		Outputs: []Output{
			&TableExistsOutput{},
		},
	}

	rule := &HasTableExistsRule{}

	assert.Nil(t, rule.Validate(tx, nil, nil))
}

func TestHasTableExistsRuleInvalid(t *testing.T) {
	tx := &Transaction{
		Outputs: []Output{
			&ColAllowedOutput{},
		},
	}

	rule := &HasTableExistsRule{}

	assert.IsType(t, errors.New(""), rule.Validate(tx, nil, nil))
}
