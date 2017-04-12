package ledger

import (
	"crypto/ecdsa"

	"github.com/satori/go.uuid"

	"github.com/wojtechnology/medblocks/crypto"
)

const userCollection = "user"

type User struct {
	Id        string
	PublicKey string
}

// TODO: Redo
func WriteUser(pub *ecdsa.PublicKey) (user *User, err error) {
	var pubEncoded []byte
	pubEncoded, err = crypto.SerializePublicKey(pub)
	if err != nil {
		return nil, err
	}

	// TODO: Don't use uuid, generate account id from hash of public key
	id := uuid.NewV4()
	user = &User{Id: id.String(), PublicKey: string(pubEncoded)}

	// c := Session.DB(DB).C(userCollection)
	// err = c.Insert(user)

	return user, err
}
