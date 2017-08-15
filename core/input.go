package core

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/wojtechnology/glacier/common"
	"github.com/wojtechnology/glacier/meddb"
)

type InputType int

const (
	INPUT_TYPE_ADMIN      InputType = iota // ADMIN      = 0
	INPUT_TYPE_WRITER                      // WRITER     = 1
	INPUT_TYPE_ROW_WRITER                  // ROW_WRITER = 2
)

type Input interface {
	OutputHash() Hash
	Type() InputType
	// TODO: Make explicit that this is a signature. For now there are no use cases of this being
	// anything else and there will probably not be.
	Data() []byte
	// TODO: Make explicit that this is a signature. For now there are no use cases of this being
	// anything else and there will probably not be.
	FromData([]byte) error
}

// ---------
// InputLink
// ---------

// Forms a link from an input to an output
type InputLink struct {
	LinksTo Hash // The hash of the output that this input links to.
}

func (link *InputLink) OutputHash() Hash {
	return link.LinksTo
}

// --------------------------------
// AdminInput implementation
//
// Allows a particular user to update a table
// --------------------------------

type AdminInput struct {
	InputLink
	Sig []byte
}

func (in *AdminInput) Type() InputType {
	return INPUT_TYPE_ADMIN
}

func (in *AdminInput) Data() []byte {
	return in.Sig
}

func (in *AdminInput) FromData(data []byte) error {
	in.Sig = data
	return nil
}

// --------------------------------
// WriterInput implementation
//
// Allows a particular user to write to a table
// --------------------------------

type WriterInput struct {
	InputLink
	Sig []byte
}

func (in *WriterInput) Type() InputType {
	return INPUT_TYPE_WRITER
}

func (in *WriterInput) Data() []byte {
	return in.Sig
}

func (in *WriterInput) FromData(data []byte) error {
	in.Sig = data
	return nil
}

// --------------------------------
// RowWriterInput implementation
//
// Allows a particular user to write to a row in a table
// --------------------------------

type RowWriterInput struct {
	InputLink
	Sig []byte
}

func (in *RowWriterInput) Type() InputType {
	return INPUT_TYPE_ROW_WRITER
}

func (in *RowWriterInput) Data() []byte {
	return in.Sig
}

func (in *RowWriterInput) FromData(data []byte) error {
	in.Sig = data
	return nil
}

// -------
// Helpers
// -------

type inputHashObject struct {
	Type       *big.Int
	OutputHash []byte
}

func HashInput(in Input) Hash {
	return rlpHash(&inputHashObject{
		Type:       intToBigInt(int(in.Type())),
		OutputHash: in.OutputHash().Bytes(),
	})
}

func toDBInput(in Input) *meddb.Input {
	return &meddb.Input{
		Type:       int(in.Type()),
		OutputHash: in.OutputHash().Bytes(),
		Data:       in.Data(),
	}
}

// Returns instance of correct input implementation for given `inputType`.
func inputFromInputType(inputType InputType, outputHash []byte) (Input, error) {
	switch inputType {
	case INPUT_TYPE_ADMIN:
		return &AdminInput{InputLink: InputLink{BytesToHash(outputHash)}}, nil
	case INPUT_TYPE_WRITER:
		return &WriterInput{InputLink: InputLink{BytesToHash(outputHash)}}, nil
	case INPUT_TYPE_ROW_WRITER:
		return &RowWriterInput{InputLink: InputLink{BytesToHash(outputHash)}}, nil
	default:
		return nil, errors.New(fmt.Sprintf("Invalid input type %d\n", inputType))
	}
}

func NewInputFromMap(inputType InputType, outputHash []byte,
	data map[string][]byte) (Input, error) {

	coreInput, err := inputFromInputType(inputType, outputHash)
	if err != nil {
		return nil, err
	}
	for fieldName, fieldValue := range data {
		if err := common.SetStructField(coreInput, fieldName, fieldValue); err != nil {
			return nil, err
		}
	}
	return coreInput, nil
}

func NewInput(inputType InputType, outputHash, data []byte) (Input, error) {
	coreInput, err := inputFromInputType(inputType, outputHash)
	if err != nil {
		return nil, err
	}
	if err := coreInput.FromData(data); err != nil {
		return nil, err
	}
	return coreInput, nil
}
