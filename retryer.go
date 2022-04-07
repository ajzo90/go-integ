package integ

import (
	"net/http"

	"github.com/ajzo90/go-requests"
)

func DefaultRetryer() requests.Doer {
	return requests.NewRetryer(http.DefaultClient, requests.Logger(func(id int, err error, msg string) {
	}))
}
