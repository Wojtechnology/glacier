package main

import "github.com/wojtechnology/glacier/meddb"

func main() {
	db, err := meddb.NewRethinkBlockchainDB([]string{"localhost"}, "prod")
	if err != nil {
		panic(err)
	}

	err = db.SetupTables()
	if err != nil {
		panic(err)
	}
}
