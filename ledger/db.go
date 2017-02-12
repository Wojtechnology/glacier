package ledger

import (
	"gopkg.in/mgo.v2"
)

var Session *mgo.Session

const DB = "medblocks"

func InitDB() (err error) {
	Session, err = mgo.Dial("127.0.0.1")
	if err != nil {
		return err
	}

	// Optional. Switch the session to a monotonic behavior.
	Session.SetMode(mgo.Monotonic, true)

	return nil
}
