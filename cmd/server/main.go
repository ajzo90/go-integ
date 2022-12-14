package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-integ/integrations/pokeapi"
	"github.com/ajzo90/go-integ/integrations/shopify"
	"github.com/ajzo90/go-integ/integrations/storm"
	"github.com/ajzo90/go-integ/pkg/airbyte"
	"github.com/ajzo90/go-integ/pkg/singer"
	"golang.org/x/crypto/nacl/sign"
	"log"
	"net/http"
	"strings"
	"time"
)

var loaders = integ.Loaders{
	"shopify": shopify.Source,
	"storm":   storm.Loader,
	"poke":    pokeapi.Poke,
}

var protos = integ.Protos{
	"":       airbyte.Proto,
	"singer": singer.Proto,
}

type Token struct {
	ExpiresAt   int64  `json:"e"`
	UrlPrefixes string `json:"u"`
	Public      []byte `json:"p"`
}

var encoding = base64.RawURLEncoding

func checkPrefixes(path string, prefixes string) bool {
	for {
		before, after, hasNext := strings.Cut(prefixes, ",")
		if strings.HasPrefix(path, before) {
			return true
		} else if !hasNext {
			return false
		}
		prefixes = after
	}
}

func verify(tok *Token, auth string, path string, allowed []*[32]byte) error {
	msg, err := encoding.DecodeString(auth)
	if err != nil {
		return err
	} else if len(msg) < sign.Overhead {
		return fmt.Errorf("invalid auth len")
	} else if err := json.Unmarshal(msg[sign.Overhead:], tok); err != nil {
		return err
	} else if time.Since(time.Unix(tok.ExpiresAt, 0)) > 0 {
		return fmt.Errorf("token expired")
	} else if checkPrefixes(path, tok.UrlPrefixes) {
		return fmt.Errorf("invalid prefix")
	}

	var isOk = false
	for _, v := range allowed {
		isOk = isOk || bytes.Equal(v[:], tok.Public)
	}

	if !isOk {
		return fmt.Errorf("invalid pk")
	}

	var pk [32]byte
	copy(pk[:], tok.Public)
	_, ok := sign.Open(nil, msg, &pk)
	if !ok {
		return fmt.Errorf("not ok")
	}
	return nil
}

func auth(r *http.Request, tok *Token, allowed []*[32]byte) error {
	return verify(tok, r.Header.Get("Authorization"), r.URL.Path, allowed)
}

func signToken(tok Token, priv *[64]byte) string {

	js, err := json.Marshal(tok)
	if err != nil {
		return ""
	}
	signed := sign.Sign(nil, js, priv)
	return encoding.EncodeToString(signed)
}

func main() {

	pub, priv, _ := sign.GenerateKey(rand.Reader)
	var tok = Token{ExpiresAt: time.Now().Add(time.Hour).Unix(), UrlPrefixes: "/poke/spec", Public: pub[:]}

	fmt.Println("Tok", tok)
	fmt.Println("Authorization:", signToken(tok, priv))

	h := integ.Handler(loaders, protos)
	authH := func(writer http.ResponseWriter, request *http.Request) {
		var tok Token
		if err := auth(request, &tok, []*[32]byte{pub}); err != nil {
			log.Println("auth error", err.Error())
			http.Error(writer, "auth error", http.StatusMethodNotAllowed)
		} else {
			h(writer, request)
		}
	}

	log.Println(http.ListenAndServe(":9900", http.HandlerFunc(authH)))
}
