package storm

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-requests"
)

var Loader = integ.New(config{}).
	Add(orders, Runner("Orders/Orders")).
	Add(customers, Runner("Customers/Customers")).
	Add(items, Runner("Products/ProductSkus"))

type config struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Url      string `json:"url"`
}

var doer = requests.NewRetryer(http.DefaultClient, requests.Logger(func(id int, err error, msg string) {
}))

func Runner(path string) integ.Runner {
	return &runner{path: path}
}

type runner struct {
	path string
}

func (s *runner) Run(ctx context.Context, extractor integ.Extractor) error {
	var state struct {
		To time.Time
	}
	var config config

	if err := extractor.Load(&config, &state); err != nil {
		return err
	}
	newTo := time.Now()

	reqB := requests.New(config.Url).BasicAuth(config.User, config.Password).Extended().Doer(doer).Clone

	schema := extractor.Schema()

	req := reqB().Path(s.path).Query("$select", strings.Join(schema.FieldKeys(), ","))

	for name, p := range schema.JsonSchema.Properties {
		for _, typ := range p.Type {
			if typ == "object" {
				req.Query("$expand", fmt.Sprintf("%s($select=%s)", name, strings.Join(p.Required, ",")))
			}
		}
	}

	for resp := new(requests.JSONResponse); ; {
		if err := extractor.Batch(ctx, req, resp, "value"); err != nil {
			return err
		} else if next := resp.String("@odata.nextLink"); next == "" {
			state.To = newTo
			return extractor.State(state)
		} else {
			req = reqB().Url(next)
		}
	}
}

//
