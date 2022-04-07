package sitoo

import (
	"net/http"
	"strings"
	"time"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var Source = integ.NewSource(config{}).
	Add(users, Runner("users")).
	Add(orders, Runner("orders"))

type config struct {
	AccountId  string `json:"account_id"`
	SiteId     string `json:"site_id"`
	ApiId      string `json:"api_id"`
	Password   string `json:"password"`
	BaseURL    string `json:"base_url"`
	TotalCount int    `json:"total_count"`
	Url        string `json:"url"`
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
	const num = 10 //default:10, max:unknown
	var count, start int
	var state struct {
		To time.Time
	}
	var config config

	if err := ctx.Load(&config, &state); err != nil {
		return err
	}
	newTo := time.Now()

	reqB := requests.New(config.Url).BasicAuth(config.ApiId, config.Password).Extended().Doer(doer).Clone

	schema := ctx.Schema()

	for resp := new(requests.JSONResponse); ; {
		req := s.updateQ(reqB(), config, schema, num, start)
		if err := ctx.EmitBatch(req, resp, "value"); err != nil {
			return err
		} else if start+num > resp.Int("totalcount") {
			state.To = newTo
			return ctx.EmitState(state)
		} else {
			count = count + num
			start = count
		}
	}
}

func (s *runner) updateQ(reqB *requests.Request, config config, schema integ.Schema, num int, start int) *requests.Request {
	return reqB.
		Path(config.AccountId+"/sites/"+config.SiteId+"/"+s.path+".json").
		Query("fields", strings.Join(schema.FieldKeys(), ",")).
		Query("num", num).
		Query("start", start)
}
