package common

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

func Now() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
}

func SetStructField(o interface{}, fieldName string, fieldValue interface{}) error {
	oValue := reflect.ValueOf(o).Elem()

	if oValue.Kind() != reflect.Struct {
		return errors.New(fmt.Sprintf("Cannot set field of object %v\n", oValue.Type()))
	}

	field := oValue.FieldByName(fieldName)
	newValue := reflect.ValueOf(fieldValue)
	if field.IsValid() && field.CanSet() {
		if field.Type() != newValue.Type() {
			return errors.New(fmt.Sprintf("Invalid type %v for field %s (%v)",
				newValue.Type(), fieldName, field.Type()))
		}
		field.Set(newValue)
	} else {
		return errors.New(fmt.Sprintf("Invalid field %s for type %v\n",
			fieldName, oValue.Type()))
	}

	return nil
}
