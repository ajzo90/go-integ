package xml

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"

	"github.com/valyala/fastjson"
)

type p struct {
	prefix               []string
	arrays               [][]string
	attrPrefix           string
	implodeArr           bool
	errOnUndeclaredArray bool

	arena fastjson.Arena
	stack
}

type stack struct {
	path   []string
	values []*fastjson.Value
}

func (p *stack) top() *fastjson.Value {
	if len(p.values) == 0 {
		return nil
	}
	return p.values[len(p.values)-1]
}

func (p *stack) push(name string, v *fastjson.Value) {
	p.path = append(p.path, name)
	p.values = append(p.values, v)
}

func (p *stack) pop() {
	p.values = p.values[:len(p.values)-1]
	p.path = p.path[:len(p.path)-1]
}

func (p *stack) topPath() string {
	if len(p.path) == 0 {
		return ""
	}
	return p.path[len(p.path)-1]
}

func (p *stack) topPathExt() string {
	if cap(p.path) > len(p.path) {
		return p.path[:cap(p.path)][len(p.path)]
	}
	return ""
}

func (p *p) ParseBytes(b []byte) (*fastjson.Value, error) {
	return p.ParseXmlToJson(bytes.NewReader(b))
}

func Decoder(prefix string, arrs []string, implodeArr bool) *p {
	p := &p{
		stack: stack{
			values: make([]*fastjson.Value, 0, 32),
			path:   make([]string, 0, 32),
		},
		implodeArr: implodeArr,
		attrPrefix: "@",
	}

	if len(prefix) > 0 {
		p.prefix = strings.Split(prefix, ".")
	}

	for _, arr := range arrs {
		p.arrays = append(p.arrays, append(append([]string{""}, p.prefix...), strings.Split(arr, ".")...))
	}

	return p
}

func (p *p) ParseXmlToJson(r io.Reader) (*fastjson.Value, error) {
	d := xml.NewDecoder(r)
	p.arena.Reset()

	str := func(n xml.Name) string {
		return n.Local
	}

	root := p.arena.NewObject()
	p.push("", root)

	eq := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	implode := func(v *fastjson.Value) {
		tp := p.topPath()
		p.pop()
		p.top().Set(tp, v)
		p.push(tp, nil)
	}

	isArr := func(name string) bool {
		pp := p.path
		pp = append(pp, name)
		for _, v := range p.arrays {
			if eq(v, pp) {
				return true
			}
		}
		return false
	}

	var char []byte

	for {
		tok, err := d.Token()
		if err == io.EOF {
			return root.Get(p.prefix...), nil
		} else if err != nil {
			return root, err
		}
		switch v := tok.(type) {
		case xml.StartElement:
			char = char[:0]

			obj := p.arena.NewObject()
			t := p.top()
			name := str(v.Name)

			if isArr(name) {
				v := t.Get(name)
				if len(v.GetArray()) == 0 {
					arr := p.arena.NewArray()
					t.Set(name, arr)
					v = arr
				}
				v.SetArrayItem(len(v.GetArray()), obj)
			} else if v := t.Get(name); v == nil || !p.errOnUndeclaredArray {
				t.Set(name, obj)
			} else {
				return nil, fmt.Errorf("undeclared array(duplicate elements) at path %s", strings.Join(p.path, ","))
			}

			for _, attr := range v.Attr {
				obj.Set(p.attrPrefix+str(attr.Name), p.arena.NewString(attr.Value))
			}

			p.push(name, obj)
		case xml.EndElement:
			if ext := p.topPathExt(); isArr(ext) {
				if p.implodeArr {
					implode(p.top().Get(ext))
				}
			} else if p.top().GetObject().Len() == 0 {
				if len(char) > 30 {
					char = char[:30]
				}
				implode(p.arena.NewStringBytes(char))
			}
			char = char[:0]
			p.pop()
		case xml.CharData:
			char = append(char, v...)
		}
	}
}
