package pokeapi

import (
	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
)

type config struct {
	Url string
}

type pokemon struct {
	Name string `json:"name"`
	Url  string `json:"url"`
}

var runner = integ.RunnerFunc(func(ctx integ.StreamContext) error {
	var cnf config
	if err := ctx.Load(&cnf, nil); err != nil {
		return err
	}

	req := requests.New(cnf.Url).
		Path("pokemon").
		Query("limit", "100")

	for resp := new(requests.JSONResponse); ; {
		if err := ctx.EmitBatch(req, resp, "results"); err != nil {
			return err
		} else if next := resp.String("next"); next == "" {
			return nil
		} else if req, err = requests.FromRawURL(next); err != nil {
			return err
		}
	}
})

var Poke = integ.NewSource(config{}).
	Add(integ.NonIncremental("pokemon", pokemon{}), runner)
