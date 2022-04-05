package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-integ/integrations/shopify"
	"github.com/ajzo90/go-integ/integrations/storm"
	"github.com/ajzo90/go-integ/pkg/airbyte"
)

var loaders = map[string]integ.Loader{
	"shopify": shopify.Loader,
	"storm":   storm.Loader,
}

var protos = integ.Protos{
	"":     airbyte.Proto,
	"mock": airbyte.Proto,
}

func main() {
	h := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/discover") {
			var o []string
			for k := range loaders {
				o = append(o, k)
			}
			if err := json.NewEncoder(writer).Encode(o); err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		for name, loader := range loaders {
			if strings.HasPrefix(request.URL.Path, "/"+name+"/") {
				integ.Server(loader, protos).ServeHTTP(writer, request)
				return
			}
		}
		http.Error(writer, "", http.StatusMethodNotAllowed)
	})

	log.Println(http.ListenAndServe(":9900", h))
}
