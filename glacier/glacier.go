package main

func ioLoop(errChannel chan<- error) {
	for true {
	}
}

func reassignTransactionsLoop(errChannel chan<- error) {
	for true {
	}
}

func addBlocksLoop(errChannel chan<- error) {
	for true {
	}
}

func voteOnBlocksLoop(errChannel chan<- error) {
	for true {
	}
}

func main() {
	errChannel := make(chan error)
	go ioLoop(errChannel)
	go reassignTransactionsLoop(errChannel)
	go addBlocksLoop(errChannel)
	go voteOnBlocksLoop(errChannel)

	err := <-errChannel
	panic(err)
}
