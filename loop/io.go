package loop

import (
	"net/http"

	"github.com/wojtechnology/glacier/core"
	"github.com/wojtechnology/glacier/handler"
)

func IOLoop(bc *core.Blockchain, errChannel chan<- error) {
	handler.SetBlockchain(bc)
	handler.SetupRoutes()
	http.ListenAndServe(":8000", nil)
}
