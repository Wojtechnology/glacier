package main

import (
	"fmt"
	"os"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/logging"
	"github.com/wojtechnology/glacier/meddb"
)

// Writes a genesis block that contains a transaction with GENESIS_MESSAGE to the blockchain.
// This will be the first block in the blockchain and will be automatically approved, without
// any votes. All vote chains will point back to this block.
func writeGenesis(bc *core.Blockchain) error {
	gen, err := bc.BuildGenesis()
	if err != nil {
		return err
	}
	gen.State = core.BLOCK_STATE_ACCEPTED

	message := gen.Transactions[0].Cols["message"].Data
	logging.Info(fmt.Sprintf("Writing genesis block with message \"%s\"...", string(message)))
	err = bc.WriteBlock(gen)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: glacier-setup <priv_key_file>")
		os.Exit(1)
	}
	logging.InitLoggers(os.Stdout, os.Stderr)
	logging.Info("Setting up glacier on localhost...")

	// Check inputs before doing anything
	me, err := core.NewNodeFromFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	db, err := meddb.NewRethinkBlockchainDB([]string{"localhost"}, "prod")
	if err != nil {
		panic(err)
	}

	logging.Info("Creating database and tables...")
	err = db.SetupTables()
	if err != nil {
		panic(err)
	}

	bc, err := core.InitBlockchain(me)
	if err != nil {
		panic(err)
	}

	err = writeGenesis(bc)
	if err != nil {
		panic(err)
	}
	logging.Info("Successfully set up glacier on local host.")
}
