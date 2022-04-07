package klaviyomembers

import (
	"net/http"
	"strings"
	"time"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var Source = integ.NewSource(config{}).
	Add(profiles, Runner("members"))

type config struct {
	ApiKey string `json:"api_id"`
	Url    string `json:"url" hint:"https://a.klaviyo.com/api/"`
	ListId string `json:"list_id"`
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
	var marker int
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
		req := s.updateQ(reqB(), config, schema, marker)
		if err := ctx.EmitBatch(req, resp, "value"); err != nil {
			return err
		} else if marker == resp.Int("marker") {
			state.To = newTo
			return ctx.EmitState(state)
		}
	}
}

func (s *runner) updateQ(reqB *requests.Request, config config, schema integ.Schema, marker int) *requests.Request {
	if marker > 0 {
		return reqB.
			Path("v2/"+"group/"+config.ListId+"/"+s.path+"/all").
			Query("fields", strings.Join(schema.FieldKeys(), ",")).
			Query("marker", marker).
			Query("api_key", config.ApiKey)
	}
	return reqB.
		Path("v2/"+"group/"+config.ListId+"/"+s.path+"/all").
		Query("fields", strings.Join(schema.FieldKeys(), ",")).
		Query("api_key", config.ApiKey)

}
