package merkle

import "fmt"

type AlreadyExistsError struct {
	Key  []byte
	Node MerkleNode
}

func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("Key \"%s\" already exists in node %s\n", hexToAscii(e.Key), e.Node)
}

type NotFoundError struct {
	Key []byte
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("Key \"%s\" does not exist in trie\n", e.Key)
}

func hexToAscii(s []byte) []byte {
	newS := make([]byte, len(s))
	for i, elem := range s {
		if elem > 9 {
			newS[i] = elem - 10 + 65
		} else {
			newS[i] = elem + 48
		}
	}
	return newS
}
