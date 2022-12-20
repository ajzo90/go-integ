package airbyte

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strings"

	"github.com/ajzo90/go-integ"
)

func Source(loader integ.Loader) {
	if err := cmd(os.Args, loader, os.Stdout); err != nil {
		log.Fatalln(err)
	}
}

func cmd(args []string, loader integ.Loader, w io.Writer) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: %s cmd [--config config] [--state state] [--catalog catalog]", args[0])
	}
	cmd, args := integ.Command(args[1]), args[2:]

	switch cmd {
	case integ.CmdSpec, integ.CmdCheck, integ.CmdRead, integ.CmdDiscover:
	default:
		return fmt.Errorf("invalid command '%s'", cmd)
	}

	b := bytes.NewBuffer(nil)
	enc := json.NewEncoder(b)
	if err := enc.Encode(map[string]any{"type": "SETTINGS", "settings": map[string]interface{}{"format": "airbyte"}}); err != nil {
		return err
	}

	for i, p := range args {
		if len(args) <= i+1 || !strings.HasPrefix(p, "--") {
			continue
		}

		m := map[string]interface{}{}
		b, err := os.ReadFile(args[i+1])
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
		if err := enc.Encode(map[string]any{"type": typ, key: m}); err != nil {
			return err
		}
	}

	return loader.Handle(context.Background(), cmd, w, bytes.NewReader(b.Bytes()), integ.Protos{
		"airbyte": Airbyte,
	})
}
