package storm

import (
	"fmt"
	"strings"
	"time"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var Loader = integ.NewSource(config{}).
	AddStream(orders, Runner("Orders/Orders")).
	AddStream(customers, Runner("Customers/Customers")).
	AddStream(items, Runner("Products/ProductSkus"))

type config struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Url      string `json:"url"`
}

func Runner(path string) integ.Runner {
	return &runner{path: path}
}

type runner struct {
	path string
}

func (s *runner) Run(ctx integ.StreamContext) error {
	var state struct {
		To time.Time
	}
	var config config

	if err := ctx.Load(&config, &state); err != nil {
		return err
	}
	newTo := time.Now()

	reqB := requests.New(config.Url).BasicAuth(config.User, config.Password).Extended().Doer(integ.DefaultRetryer()).Clone

	schema := ctx.Schema()

	req := reqB().Path(s.path).Query("$select", strings.Join(schema.FieldKeys(), ","))

	for name, p := range schema.JsonSchema.Properties {
		for _, typ := range p.Type {
			if typ == "object" {
				req.Query("$expand", fmt.Sprintf("%s($select=%s)", name, strings.Join(p.Required, ",")))
			}
		}
	}

	for resp := new(requests.JSONResponse); ; {
		if err := ctx.EmitBatch(req, resp, "value"); err != nil {
			return err
		} else if next := resp.String("@odata.nextLink"); next == "" {
			state.To = newTo
			return ctx.EmitState(state)
		} else {
			req = reqB().Url(next)
		}
	}
}

//
