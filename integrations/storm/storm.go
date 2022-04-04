package storm

import (
	"context"
	"fmt"
	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-requests"
	"net/http"
	"strings"
	"time"
)

// https://stormdocs.atlassian.net/servicedesk/customer/portal/1/article/2215706817
// https://query.lab.storm.io/2.0/Docs/Index#/Orders/Entities/OrderItem creds(21:XXX)
var orders = integ.Incremental("orders", struct {
	Id        int32
	OrderNo   float64
	OrderDate string
}{}).Primary(integ.Field("Id"))

var customers = integ.Incremental("users", struct {
	Id           int32
	Key          string
	EmailAddress string
	IsActive     bool
}{}).Primary(integ.Field("Id"))

var items = integ.Incremental("items", struct {
	StatusId  int
	PartNo    string
	IsBuyable bool
	Product   Product
}{})

type Product struct {
	Id                 int
	ManufacturerId     int
	ManufacturerPartNo string
}

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
	var newTo = time.Now()

	var reqB = requests.New(config.Url).BasicAuth(config.User, config.Password).Extended().Clone

	var schema = extractor.Schema()

	req := reqB().
		Path(s.path).
		Query("$select", strings.Join(schema.FieldKeys(), ","))

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
