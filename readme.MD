# go-integ (final name TBD)

Makes it simple to pull data from HTTP based data sources. Support singer and airbyte protocol, and can easily be
extended to a custom protocol format.

## Example integration

Shopify source with incremental sync

```go
package shopify

import (
	"context"
	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-requests"
	"strings"
	"time"
)

type config struct {
	ApiKey integ.MaskedString `json:"api_key"`
	Url    string             `json:"url" default:"x" hint:"https://xxx.myshopify.com/admin/api/2021-10/"`
}

func (config *config) request() *requests.Request {
	return requests.New(config.Url).
		SecretHeader("X-Shopify-Access-Token", config.ApiKey).
		Extended().Clone()
}

type Runner string

func (r Runner) Run(ctx context.Context, extractor integ.Extractor) error {
	var state struct {
		To time.Time
	}
	var config config
	if err := extractor.Load(&config, &state); err != nil {
		return err
	}

	from, to := state.To, time.Now()

	var req = config.request().
		Path(s.path+".json").
		Query("updated_at_min", from.Format(time.RFC3339)).
		Query("updated_at_max", to.Format(time.RFC3339)).
		Query("fields", strings.Join(extractor.Schema().FieldKeys(), ",")).
		Query("status", "any")

	for resp := new(requests.JSONResponse); ; {
		if err := extractor.Batch(ctx, req, resp, r); err != nil {
			return err
		} else if next := ParseNext(resp.Header("link")); next == "" {
			state.To = to
			return extractor.State(state)
		} else {
			req = config.request().Url(next)
		}
	}
}

func ParseNext(s string) string {
	return s // shopify specific ...
}

var Loader = integ.New(config{}).
	Add(users, Runner("customers")).
	Add(orders, Runner("orders"))

var users = integ.Incremental("users", struct {
	ID    int    `json:"id"`
	Email string `json:"email"`
	...
}{}).Primary(integ.Field("id"))

var orders = integ.Incremental("orders", struct {
	Id    string  `json:"id"`
	Price float64 `json:"price"`
	...
}{}).Primary(integ.Field("id"))

// ... and more streams/types..

```

## Throttling, retries and request control

`github.com/ajzo90/go-requests` is using a request builder where the `Doer` (see below, but think client
abstraction) can be injected to have full control over the http details. A common use case is to use a single doer(
client) that is shared for all requests, and that can be responsible for not sending to many requests.

The `requests.NewRetryer` is a nice default that handles `429` related headers combined with exponential backoff/retry
for common transient http statuses. The `Doer` interface is easy to adjust according to specific needs.
See `github.com/ajzo90/go-requests` for more details.

```go
var doer = requests.NewRetryer(http.DefaultClient, requests.Logger(func (id int, err error, msg string) {
    // log intermediate errors here if you like 
}))
```

```go
type Doer interface {
// Do attempt to do one http request (including retries/redirects)
Do(r *http.Request) (*http.Response, error)
}
```

## secrets and logging

...

