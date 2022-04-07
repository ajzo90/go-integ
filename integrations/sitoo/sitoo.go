package sitoo

import (
	"strconv"
	"strings"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var Source = integ.NewSource(config{}).
	HttpRunner(Runner).
	HttpStream(users).
	HttpStream(orders).
	Documentation("https://developer.sitoo.com").
	Notes(`prototype/draft`)

var users = integ.NonIncremental("users", struct {
	UserId  string `json:"userid"`
	Email   string `json:"email"`
	Company string `json:"company"`
}{})

var orders = integ.NonIncremental("orders", struct {
	OrderId int    `json:"orderid"`
	Email   string `json:"email"`
}{})

type config struct {
	Url       string `hint:"https://api.mysitoo.com/v2/"`
	AccountId string
	SiteId    string
	ApiId     string
	Password  string
	Num       int
}

// todo: implement incremental sync

var Runner = integ.RunnerFunc(func(ctx integ.HttpContext) error {
	cnf := config{Num: 10}
	if err := ctx.Load(&cnf, nil); err != nil {
		return err
	}

	var start int

	req := requests.New(cnf.Url).
		BasicAuth(cnf.ApiId, cnf.Password).
		Path("accounts/"+cnf.AccountId+"/sites/"+cnf.SiteId+"/"+ctx.Schema().Name+".json").
		Query("fields", strings.Join(ctx.Schema().FieldKeys(), ",")).
		Query("num", cnf.Num).
		Query("start", func() string { return strconv.Itoa(start) }).
		Extended().Doer(integ.DefaultRetryer()).Clone()

	for resp := new(requests.JSONResponse); ; start += cnf.Num {
		if err := ctx.EmitBatch(req, resp, "value"); err != nil {
			return err
		} else if len(resp.GetArray("value")) < cnf.Num {
			return nil
		}
	}
})
