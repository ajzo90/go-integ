package magento

import (
	"context"
	"fmt"
	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-requests"
)

// status (ajzo90): not working. the api is terrible and I dont think it is possible to iterate large collections??

type config struct {
	DomainName string
	StoreCode  string
	Username   string `json:"username"`
	Password   string `json:"password"`
}

type runner struct {
	path string
}

func (c *config) url(p string) string {
	var s = c.StoreCode
	if s == "" {
		s = "default"
	}
	var proto = "https"
	if c.DomainName == "localhost" {
		proto = "http"
	}
	return fmt.Sprintf("%s://%s%s%s/%s", proto, c.DomainName, "/index.php/rest/", s, p)
}

func generateToken(url, user, pw string) (string, error) {
	resp, err := requests.NewPost(url).JSONBody(map[string]string{
		"username": user,
		"password": pw,
	}).ExecJSON(context.Background())
	if err != nil {
		return "", err
	}
	return resp.String(), nil
}

func (s *runner) Run(ctx context.Context, loader integ.StreamLoader) error {

	var config config
	if err := loader.Load(&config, nil); err != nil {
		return err
	}

	token, err := generateToken(config.url("V1/integration/admin/token"), config.Username, config.Password)
	if err != nil {
		return err
	}

	var _ = requests.New(config.url("V1")).
		Path(s.path). // orders
		Header("Authorization", "Bearer "+token).
		Query("searchCriteria", "all").
		Header("accept", "application/json").
		Extended().Clone

	//for {
	//	if resp, err := loader.WriteBatch(ctx, v()); err != nil {
	//		return err
	//	}
	//
	//}

	return nil
}
