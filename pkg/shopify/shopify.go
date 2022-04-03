package shopify

import (
	"context"
	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-requests"
	"net/http"
	"strings"
	"time"
)

var Loader = integ.NewLoader(config{}).
	Stream("users", Runner("customers"), user{}).
	Stream("items", Runner("products"), item{}).
	Stream("orders", Runner("orders"), order{})

type config struct {
	ApiKey     string `json:"api_key" formType:"secret"`
	Url        string `json:"url" formType:"url" hint:"https://xxx.myshopify.com/admin/api/2021-10/"`
	ApiVersion string `json:"api_version" options:"[2021-10]"`
}

func Runner(path string) integ.Runner {
	return &runner{path: path}
}

type runner struct {
	path string
}

func (s *runner) Run(ctx context.Context, loader integ.Loader) error {
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
		Query("fields", strings.Join(loader.Fields(), ",")).
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
