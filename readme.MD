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

Pokeapi source with (mocked) incremental sync, see more examples in /integrations folder.

```go
package pokeapi

import (
	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var Poke = integ.NewSource(config{}).
	HttpStream(integ.Incremental("pokemon", pokemon{}), runner)

type config struct {
	Url string
}

type pokemon struct {
	Name string `json:"name"`
	Url  string `json:"url"`
}

var runner = integ.RunnerFunc(func(ctx integ.HttpContext) error {
	var cnf config
	var dummyState struct{}
	if err := ctx.Load(&cnf, &dummyState); err != nil {
		return err
	}

	req := requests.New(cnf.Url).
		Path("pokemon").
		Query("limit", "100")

	resp := new(requests.JSONResponse)
	for {
		if err := ctx.EmitBatch(req, resp, "results"); err != nil {
			return err
		} else if next := resp.String("next"); next == "" {
			return ctx.EmitState(dummyState)
		} else if req, err = requests.FromRawURL(next); err != nil {
			return err
		}
	}
})

```

## The contract to implement

To implement a source connector you have to fulfill the `Runner` interface

```go
type HttpRunner interface {
	// Run runs the sync job.
	Run(ctx HttpContext) error
}
```

where the `Extractor` argument is defined by the following interface

```go

type HttpContext interface {
	// Load a stream with shared config and state
	Load(config, state interface{}) error

	Schema() Schema

	// EmitState emit the state
	EmitState(v interface{}) error

	// EmitBatch executes the provided request, locate the data array and emit the records
	// (likely) called multiple times in the same run
	// resp: (pre-allocated and reusable)
	// path: (path to the data array)
	EmitBatch(req *requests.Request, resp *requests.JSONResponse, path ...string) error
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

## Airbyte source example (shopify)
```shell
docker build -t airbyte-source-shopify:dev -f dockerfile-airbyte-source-shopify .
docker run --rm airbyte-source-shopify:dev spec
...
```

