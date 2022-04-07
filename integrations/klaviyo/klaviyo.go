package klaviyomembers

import (
	"strconv"
	"strings"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

var user = struct {
	Id    string `json:"id"`
	Email string `json:"email"`
}{}

var Source = integ.NewSource(config{}).
	AddStream(integ.NonIncremental("members", user), RunnerV2("members")).
	Documentation("https://developers.klaviyo.com/en/reference/get-members").
	Notes(`prototype/draft`)

type config struct {
	ApiKey integ.MaskedString `json:"api_id"`
	Url    string             `json:"url" hint:"https://a.klaviyo.com/api/"`
	ListId string             `json:"list_id"`
}

type RunnerV2 string

func (s RunnerV2) Run(ctx integ.StreamContext) error {
	var config config
	if err := ctx.Load(&config, nil); err != nil {
		return err
	}

	req := requests.New(config.Url).
		Path("v2/"+"group/"+config.ListId+"/"+string(s)+"/all").
		Query("api_key", config.ApiKey).
		Query("fields", strings.Join(ctx.Schema().FieldKeys(), ",")).
		Extended().Doer(integ.DefaultRetryer()).Clone()

	for resp := new(requests.JSONResponse); ; {
		if err := ctx.EmitBatch(req, resp, "records"); err != nil {
			return err
		} else if marker := resp.Int("marker"); marker == 0 {
			return nil
		} else {
			req = req.Query("marker", strconv.Itoa(marker))
		}
	}
}
