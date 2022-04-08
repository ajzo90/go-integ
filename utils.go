package integ

import (
	"net/http"
	"sort"
	"strings"

	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/ajzo90/go-requests"
)

type MaskedString string

func (s MaskedString) String() string {
	return string(s)
}

func (s MaskedString) Masked() string {
	return strings.Repeat("x", len(s))
}

func (s MaskedString) MarshalJSON() ([]byte, error) {
	return []byte(s.Masked()), nil
}

func Keys(schema *jsonschema.Document) []string {
	var o []string
	for k := range schema.Properties {
		o = append(o, k)
	}
	sort.Strings(o)
	return o
}

func DefaultRetryer() requests.Doer {
	return requests.NewRetryer(http.DefaultClient, requests.Logger(func(id int, err error, msg string) {
	}))
}
