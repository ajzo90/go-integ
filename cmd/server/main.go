package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/ajzo90/go-integ"

	"github.com/ajzo90/go-integ/integrations/shopify"
	"github.com/ajzo90/go-integ/integrations/storm"
)

var loaders = map[string]go_integ.Loader{
	"shopify": shopify.Loader,
	"storm":   storm.Loader,
}

var protos = go_integ.Protos{
	"":     go_integ.AirbyteProto,
	"mock": go_integ.AirbyteProto,
}

func main() {
	for name, loader := range loaders {
		if err := loader.Validate(); err != nil {
			panic(fmt.Errorf("validation error in %s: %v", name, err))
		}
	}

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

		for key, loader := range loaders {
			if strings.HasPrefix(request.URL.Path, "/"+key+"/") {
				go_integ.Server(loader, protos).ServeHTTP(writer, request)
				return
			}
		}
		http.Error(writer, "", http.StatusMethodNotAllowed)
	})

	log.Println(http.ListenAndServe(":9900", h))
}
