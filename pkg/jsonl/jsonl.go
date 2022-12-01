package jsonl

import (
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
)

type jsonl struct {
	s fastjson.Scanner
	a fastjson.Arena
}

func NewParser() requests.JSONParser {
	return &jsonl{}
}

func (j *jsonl) ParseBytes(bytes []byte) (*fastjson.Value, error) {
	j.a.Reset()
	j.s.InitBytes(bytes)

	root := j.a.NewArray()

	for i := 0; j.s.Next(); i++ {
		root.SetArrayItem(i, j.s.Value())
	}

	return root, j.s.Error()
}
