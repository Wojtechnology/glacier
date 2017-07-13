package core

import "github.com/wojtechnology/glacier/meddb"

// -------------------
// Transaction Changes
// -------------------

type TransactionChange struct {
	NewTransaction *Transaction
	OldTransaction *Transaction
}

// Wrapper around a meddb transaction that maps meddb transactions to core transactions.
type TransactionChangeCursor struct {
	changefeed meddb.TransactionChangefeed
}

func (cursor *TransactionChangeCursor) Next(change *TransactionChange) bool {
	var res meddb.TransactionChangefeedRes

	changed := cursor.changefeed.Next(&res)
	if changed {
		if res.NewVal != nil {
			change.NewTransaction = fromDBTransaction(res.NewVal)
		} else {
			change.NewTransaction = nil
		}
		if res.OldVal != nil {
			change.OldTransaction = fromDBTransaction(res.OldVal)
		} else {
			change.OldTransaction = nil
		}
	}

	return changed
}
