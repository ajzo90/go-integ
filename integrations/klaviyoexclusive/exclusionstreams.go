package klaviyoexclusive

import (
	"net/http"
	"strings"
	"time"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var Source = integ.NewSource(config{}).
	Add(exclusions, Runner("exclusions"))

type config struct {
	ApiKey string `json:"api_id"`
	Url    string `json:"url" hint:"https://a.klaviyo.com/api/"`
}

var doer = requests.NewRetryer(http.DefaultClient, requests.Logger(func(id int, err error, msg string) {
}))

func Runner(path string) integ.Runner {
	return &runner{path: path}
}

type runner struct {
	path string
}

func (s *runner) Run(ctx integ.StreamContext) error {
	var page, count int
	var state struct {
		To time.Time
	}
	var config config

	if err := ctx.Load(&config, &state); err != nil {
		return err
	}
	newTo := time.Now()

	reqB := requests.New(config.Url).Extended().Doer(doer).Clone

	schema := ctx.Schema()

	for resp := new(requests.JSONResponse); ; {
		req := s.updateQ(reqB(), config, schema, page)
		if err := ctx.EmitBatch(req, resp, "value"); err != nil {
			return err
		} else if count == resp.Int("total") {
			state.To = newTo
			return ctx.EmitState(state)
		} else {
			page++
			count += len(resp.GetArray("data"))
		}
	}
}

func (s *runner) updateQ(reqB *requests.Request, config config, schema integ.Schema, page int) *requests.Request {
	return reqB.
		Path("v1/"+"people/"+s.path).
		Query("fields", strings.Join(schema.FieldKeys(), ",")).
		Query("count", 500).
		Query("sort", "desc").
		Query("page", page).
		Query("api_key", config.ApiKey)
}
