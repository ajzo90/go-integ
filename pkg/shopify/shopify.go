package shopify

import (
	"context"
	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/ajzo90/go-requests"
	"net/http"
	"strings"
	"time"
)

var ShopifyLoader = integ.NewLoader(config{}).
	Stream("users", Runner("customers", user{}), user{}).
	Stream("items", Runner("products", item{}), item{})

type config struct {
	ApiKey string `json:"api_key" formType:"secret"`
	Url    string `json:"url" formType:"url"`
}

type user struct {
	ID               int    `json:"id" isKey:"true"`
	Email            string `json:"email" isHashed:"true"`
	CreatedAt        string `json:"created_at"`
	VerifiedEmail    bool   `json:"verified_email"`
	AcceptsMarketing bool   `json:"accepts_marketing"`
}

type item struct {
	Id    string  `json:"id" isKey:"true"`
	Price float64 `json:"price"`
}

func Runner(path string, v interface{}) integ.Runner {
	return &shopifyRunner{path: path, fields: integ.Keys(jsonschema.New(v))}
}

type shopifyRunner struct {
	path   string
	fields []string
}

func (s shopifyRunner) Run(ctx context.Context, loader integ.Loader) error {
	var state struct {
		To time.Time
	}
	var config config
	if err := loader.Load(&config, &state); err != nil {
		return err
	}

	from, to := timeWindow(state.To)

	var newQ = requests.
		New(config.Url).
		Method(http.MethodGet).
		SecretHeader("X-Shopify-Access-Token", config.ApiKey).
		Extended().Doer(http.DefaultClient).Clone

	var q = newQ().
		Path(s.path+".json").
		Query("updated_at_min", from.Format(time.RFC3339)).
		Query("updated_at_max", to.Format(time.RFC3339)).
		Query("fields", strings.Join(s.fields, ",")).
		Query("status", "any")

	for {
		if resp, err := loader.WriteBatch(ctx, q, s.path); err != nil {
			return err
		} else if next := ParseNext(resp.Header("link")); next == "" {
			state.To = to
			return loader.State(state)
		} else {
			q = newQ().Url(next)
		}
	}
}

func timeWindow(old time.Time) (from, to time.Time) {
	if old.IsZero() {
		old = time.Now().Add(-time.Hour * 24 * 365 * 10) // 10 years
	}
	return old, time.Now()
}

//ParseNext extract the next-link from a shopify link header, see test for further details
func ParseNext(s string) string {
	const nextRelSuffix = `; rel="next"`
	for _, part := range strings.Split(s, ", ") {
		var link = strings.TrimSuffix(part, nextRelSuffix)
		if len(link) != len(part) && len(link) > 2 && link[0] == '<' && link[len(link)-1] == '>' {
			return link[1 : len(link)-1]
		}
	}
	return ""
}
