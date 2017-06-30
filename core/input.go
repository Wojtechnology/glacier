package core

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/wojtechnology/glacier/meddb"
)

type InputType int

const (
	INPUT_TYPE_ADMIN InputType = iota // ADMIN = 1 - user can modify table
)

type Input interface {
	OutputHash() Hash
	Type() InputType
	Data() []byte
}

// Forms a link from an input to an output
type InputLink struct {
	LinksTo Hash // The hash of the output that this input links to.
}

func (link *InputLink) OutputHash() Hash {
	return link.LinksTo
}

type AdminInput struct {
	InputLink
	PubKey []byte
}

func (in *AdminInput) Type() InputType {
	return INPUT_TYPE_ADMIN
}

func (in *AdminInput) Data() []byte {
	return in.PubKey
}

// -------
// Helpers
// -------

type inputHashObject struct {
	Type       *big.Int
	OutputHash []byte
	Data       []byte
}

func hashInput(in Input) Hash {
	return rlpHash(&inputHashObject{
		Type:       intToBigInt(int(in.Type())),
		OutputHash: in.OutputHash().Bytes(),
		Data:       in.Data(),
	})
}

func toDBInput(in Input) *meddb.Input {
	return &meddb.Input{
		Type:       int(in.Type()),
		OutputHash: in.OutputHash().Bytes(),
		Data:       in.Data(),
	}
}

func fromDBInput(in *meddb.Input) (Input, error) {
	switch InputType(in.Type) {
	case INPUT_TYPE_ADMIN:
		return &AdminInput{InputLink{BytesToHash(in.OutputHash)}, in.Data}, nil
	default:
		return nil, errors.New(fmt.Sprint("Invalid input type: %d\n", in.Type))
	}
}
