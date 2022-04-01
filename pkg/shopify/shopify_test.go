package shopify

import (
	"context"
	"encoding/json"
	"github.com/ajzo90/go-requests"
	"github.com/matryer/is"
	"log"
	"os"
	"strings"
	"testing"
)

type mock struct {
}

func (m mock) Load(config, state interface{}) error {
	const configStr = `{"api_key":"", "url": "https://xxx.myshopify.com/admin/api/2021-10/"}`
	if err := json.NewDecoder(strings.NewReader(configStr)).Decode(config); err != nil {
		return err
	} else if err := json.NewDecoder(strings.NewReader(`{}`)).Decode(state); err != nil {
		return err
	}
	return nil
}

func (m mock) WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error) {
	resp, err := q.ExecJSON(ctx)
	if err != nil {
		return nil, err
	}
	q.Extended().Write(os.Stdout)
	for _, v := range resp.GetArray(keys...) {
		log.Println(v)
	}
	return resp, err
}

func (m mock) State(v interface{}) error {
	log.Println("new state", v)
	return nil
}

func TestRun(t *testing.T) {
	var r = Runner("customers", user{})
	is.New(t).NoErr(r.Run(context.Background(), mock{}))
}

/*
note:
HTTP request bodies are theoretically allowed for all methods except TRACE, however they are not commonly used except in PUT, POST and PATCH. Because of this, they may not be supported properly by some client frameworks, and you should not allow request bodies for GET, DELETE, TRACE, OPTIONS and HEAD methods.
*/
//var shopifyDoer = requests.NewRetryer(http.DefaultClient, requests.Logger(func(id int, err error, msg string) {
//	log.Println(id, err, msg)
//}))
