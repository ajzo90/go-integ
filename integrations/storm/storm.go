package storm

import (
	"fmt"
	"strings"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var Loader = integ.NewSource(config{}).
	HttpStream(orders, Runner("Orders/Orders")).
	HttpStream(customers, Runner("Customers/Customers")).
	HttpStream(items, Runner("Products/ProductSkus")).
	Documentation(
		"https://storm.io/docs/storm-api/",
		"https://stormdocs.atlassian.net/servicedesk/customer/portal/1/article/2215706817",
		"https://query.lab.storm.io/2.0/Docs/Index#/Orders/Entities/OrderItem",
		"https://documenter.getpostman.com/view/2973406/RztoKSkc",
	).
	Notes(`Incremental not implemented`)

type config struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Url      string `json:"url"`
}

func Runner(path string) integ.HttpRunner {
	return &runner{path: path}
}

type runner struct {
	path string
}

func (s *runner) Run(ctx integ.HttpContext) error {
	var config config

	if err := ctx.Load(&config, nil); err != nil {
		return err
	}
	newReq := requests.New(config.Url).BasicAuth(config.User, config.Password).Extended().Doer(integ.DefaultRetryer()).Clone

	schema := ctx.Schema()

	req := newReq().Path(s.path).Query("$select", strings.Join(schema.FieldKeys(), ","))

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
			return nil
		} else {
			req = newReq().Url(next)
		}
	}
}
