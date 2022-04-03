package magento

import (
	"context"
	"github.com/ajzo90/go-requests"
	"github.com/matryer/is"
	"log"
	"net/http"
	"testing"
)

func TestConnect(t *testing.T) {
	var config = config{Username: "christian", Password: "secret1234", DomainName: "localhost"}
	u := config.url("V1/integration/admin/token")
	tok, err := generateToken(u, config.Username, config.Password)
	is.New(t).NoErr(err)

	resp, err := requests.New(config.url("V1")).Method(http.MethodPost).
		Path("products"). // orders
		Header("Authorization", "Bearer "+tok).
		//Query("searchCriteria", "all").
		Header("accept", "application/json").
		ExecJSON(context.Background())
	log.Fatalln(string(resp.Body().MarshalTo(nil)), err)

}
