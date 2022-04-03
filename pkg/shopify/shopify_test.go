package shopify_test

import (
	"context"
	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-integ/pkg/shopify"
	"github.com/matryer/is"
	"os"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	is := is.New(t)
	p, err := integ.Open(strings.NewReader(mockData), os.Stdout)
	is.NoErr(err)
	is.NoErr(shopify.Loader.Run(context.Background(), p, true))
}

/*
note:
HTTP request bodies are theoretically allowed for all methods except TRACE, however they are not commonly used except in PUT, POST and PATCH. Because of this, they may not be supported properly by some client frameworks, and you should not allow request bodies for GET, DELETE, TRACE, OPTIONS and HEAD methods.
*/
//var shopifyDoer = requests.NewRetryer(http.DefaultClient, requests.Logger(func(id int, err error, msg string) {
//	log.Println(id, err, msg)
//}))
