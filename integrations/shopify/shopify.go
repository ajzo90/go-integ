package shopify

import (
	"strings"
	"time"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var Source = integ.NewSource(config{}).
	HttpStream(users, Runner("customers")).
	HttpStream(orders, Runner("orders"))

type config struct {
	ApiKey integ.MaskedString `json:"api_key"`
	Url    string             `json:"url" default:"x" hint:"https://xxx.myshopify.com/admin/api/2021-10/"`
}

func (config *config) request() *requests.Request {
	return requests.
		New(config.Url).
		SecretHeader("X-Shopify-Access-Token", config.ApiKey).
		Extended().Doer(integ.DefaultRetryer()).Clone()
}

func Runner(path string) integ.HttpRunner {
	return &runner{path: path}
}

type runner struct {
	path string
}

func (s *runner) Run(ctx integ.HttpContext) error {
	var state struct {
		To time.Time
	}
	var config config
	if err := ctx.Load(&config, &state); err != nil {
		return err
	}
	from, to := timeWindow(state.To)

	req := config.request().
		Path(s.path+".json").
		Query("updated_at_min", from.Format(time.RFC3339)).
		Query("updated_at_max", to.Format(time.RFC3339)).
		Query("fields", strings.Join(ctx.Schema().FieldKeys(), ",")).
		Query("status", "any")

	for resp := new(requests.JSONResponse); ; {
		if err := ctx.EmitBatch(req, resp, s.path); err != nil {
			return err
		} else if next := ParseNext(resp.Header("link")); next == "" {
			state.To = to
			return ctx.EmitState(state)
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

// ParseNext extract the next-link from a shopify link header, see test for further details
func ParseNext(s string) string {
	const nextRelSuffix = `; rel="next"`
	for _, part := range strings.Split(s, ", ") {
		link := strings.TrimSuffix(part, nextRelSuffix)
		if len(link) != len(part) && len(link) > 2 && link[0] == '<' && link[len(link)-1] == '>' {
			return link[1 : len(link)-1]
		}
	}
	return ""
}
