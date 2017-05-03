package meddb

import "fmt"

type NotFoundError struct {
	Key []byte
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("Key \"%s\" does not exist in db\n", e.Key)
}
