package ledger

// Strings, since byte arrays cannot be declared const
const (
	// Related to blocks
	blockHeaderPrefix = "h"
	blockBodyPrefix   = "b"
	headKey           = "h"

	// Related to transactions
	unspendTxOutputPrefix = "utxo"

	genesisParentHash = "FILL IN WITH SOMETHING"
)
