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
	Add(users, Runner("customers")).
	Add(orders, Runner("orders"))

type config struct {
	ApiKey        integ.MaskedString `json:"api_key" formType:"secret"`
	Url           string             `json:"url" formType:"url" hint:"https://xxx.myshopify.com/admin/api/2021-10/"`
	ApiVersion    string             `json:"api_version" options:"[2021-10]"`
	MaxWindowDays int                `json:"max_window_days"`
}

var doer = requests.NewRetryer(http.DefaultClient, requests.Logger(func(id int, err error, msg string) {

}))

func (config *config) request() *requests.Request {
	return requests.
		New(config.Url).
		SecretHeader("X-Shopify-Access-Token", config.ApiKey).
		Extended().Doer(doer).Clone()
}

func Runner(path string) integ.Runner {
	return &runner{path: path}
}

type runner struct {
	path string
}

func (s *runner) Run(ctx context.Context, loader integ.StreamLoader) error {
	var state struct {
		To time.Time
	}
	var config config
	if err := loader.Load(&config, &state); err != nil {
		return err
	}

	from, to := timeWindow(state.To)

	var req = config.request().
		Path(s.path+".json").
		Query("updated_at_min", from.Format(time.RFC3339)).
		Query("updated_at_max", to.Format(time.RFC3339)).
		Query("fields", strings.Join(loader.Schema().FieldKeys(), ",")).
		Query("status", "any")

	for resp := new(requests.JSONResponse); ; {
		if err := loader.WriteBatch(ctx, req, resp, s.path); err != nil {
			return err
		} else if next := ParseNext(resp.Header("link")); next == "" {
			state.To = to
			return loader.State(state)
		} else {
			req = config.request().Url(next)
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
