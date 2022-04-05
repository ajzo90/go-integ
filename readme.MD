# go-integ (final name TBD)

Makes it simple to pull data from HTTP based data sources. Support singer and airbyte protocol, and can easily be
extended to a custom protocol format.

[![CICD](https://github.com/ajzo90/go-integ/actions/workflows/ci.yml/badge.svg)](https://github.com/ajzo90/go-integ/actions/workflows/ci.yml)
[![CICD](https://github.com/ajzo90/go-integ/actions/workflows/go.yml/badge.svg)](https://github.com/ajzo90/go-integ/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ajzo90/go-integ)](https://goreportcard.com/report/github.com/ajzo90/go-integ)
[![GoDoc](https://godoc.org/github.com/ajzo90/go-integ?status.svg)](https://godoc.org/github.com/ajzo90/go-integ)
[![License](https://shields.io/github/license/ajzo90/go-integ)](LICENSE)
[![Latest Version](https://shields.io/github/v/release/ajzo90/go-integ?display_name=tag&sort=semver)](https://github.com/ajzo90/go-integ/releases)
[![codecov](https://codecov.io/gh/ajzo90/go-integ/branch/main/graph/badge.svg?token=BDKHJVZCUY)](https://codecov.io/gh/ajzo90/go-integ)

## WIP! Identifiers will likely change, the structure will likely be similar though.

## Example integration

Shopify source with incremental sync

```go
package shopify

import (
	"strings"
	"time"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
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

func (r Runner) Run(ctx integ.StreamContext) error {
	var state struct {
		To time.Time
	}
	var config config
	if err := ctx.Load(&config, &state); err != nil {
		return err
	}

	from, to := state.To, time.Now()

	var req = config.request().
		Path(s.path+".json").
		Query("updated_at_min", from.Format(time.RFC3339)).
		Query("updated_at_max", to.Format(time.RFC3339)).
		Query("fields", strings.Join(ctx.Schema().FieldKeys(), ",")).
		Query("status", "any")

	for resp := new(requests.JSONResponse); ; {
		if err := ctx.EmitBatch(ctx, req, resp, r); err != nil {
			return err
		} else if next := ParseNext(resp.Header("link")); next == "" {
			state.To = to
			return ctx.EmitState(state)
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
	Id    int    `json:"id"`
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

## The contract to implement

To implement a source connector you have to fulfill the `Runner` interface

```go
type Runner interface {
	// Run runs the sync job.
	Run(ctx StreamContext) error
}
```

where the `Extractor` argument is defined by the following interface

```go

type StreamContext interface {
	// Load a stream with shared config and state
	Load(config, state interface{}) error

	Schema() Schema

	// EmitBatch executes the provided request, locate the data array and emit the records
	// (likely) called multiple times in the same run
	// resp: (pre-allocated and reusable)
	// path: (path to the data array)
	EmitBatch(req *requests.Request, resp *requests.JSONResponse, path ...string) error

	// State emit the state
	EmitState(v interface{}) error
}
```

## Throttling, retries and request control

[github.com/ajzo90/go-requests](https://github.com/ajzo90/go-requests) is using a request builder where the `Doer` (see
below, but think client abstraction) can be injected to have full control over the http details. A common use case is to
use a single doer that is shared for all requests, and that can be responsible for not sending to many requests.

The `requests.NewRetryer` is a nice default that handles `429` related headers combined with exponential backoff/retry
for common transient http statuses. The `Doer` interface is easy to adjust according to specific needs.

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

