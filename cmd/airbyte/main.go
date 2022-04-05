package main

import (
	"log"
	"os"

	"github.com/ajzo90/go-integ/integrations/shopify"
	"github.com/ajzo90/go-integ/pkg/airbyte"
)

func main() {
	if err := airbyte.Cmd(os.Args, shopify.Loader, os.Stdout); err != nil {
		log.Fatalln(err)
	}
}
