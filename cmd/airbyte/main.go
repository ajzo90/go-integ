package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-integ/integrations/shopify"
)

func main() {
	if err := Main(os.Args, shopify.Loader); err != nil {
		log.Fatalln(err)
	}
}

func Main(args []string, loader integ.Loader) error {
	if err := loader.Validate(); err != nil {
		return fmt.Errorf("validation error in %s: %v", args[0], err)
	} else if len(args) < 2 {
		return fmt.Errorf("usage: airbyte cmd [--config config] [--state state] [--catalog catalog]")
	}
	cmd := args[1]
	args = args[2:]

	b := bytes.NewBuffer(nil)
	enc := json.NewEncoder(b)
	if err := enc.Encode(map[string]interface{}{"type": "SETTINGS", "settings": map[string]interface{}{"format": "airbyte"}}); err != nil {
		return err
	}

	for i, p := range args {
		if len(args) <= i+1 || !strings.HasPrefix(p, "--") {
			continue
		}

		m := map[string]interface{}{}
		b, err := ioutil.ReadFile(args[i+1])
		if errors.Is(err, fs.ErrNotExist) {
			b = []byte(args[i+1])
		} else if err != nil {
			return err
		}

		if err := json.Unmarshal(b, &m); err != nil {
			return err
		}

		var typ, key string
		switch p {
		case "--config":
			typ, key = "CONFIG", "config"
		case "--state":
			typ, key = "STATE", "state"
		case "--catalog":
			typ, key = "CATALOG", "catalog"
		}
		if err := enc.Encode(map[string]interface{}{"type": typ, key: m}); err != nil {
			return err
		}
	}

	return loader.Handle(context.Background(), integ.Command(cmd), os.Stdout, bytes.NewReader(b.Bytes()), integ.Protos{
		"airbyte": integ.AirbyteProto,
	})
}
