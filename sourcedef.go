package integ

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/ajzo90/go-jsonschema-generator"
	"golang.org/x/sync/errgroup"
)

type sourceDef struct {
	incremental bool
	config      interface{}
	runners     runners
	protos      Protos
	httpRunner  HttpRunner
	notes       []string
	docs        []string
	version     string
}

func (r *sourceDef) Handle(ctx context.Context, cmd Command, writer io.Writer, rd io.Reader, protos Protos) error {
	proto, err := Open(rd, writer, cmd, protos)
	if err != nil {
		return err
	}

	err = r.handle(ctx, proto, cmd)
	closeErr := proto.Close()

	if err != nil {
		return err
	} else {
		return closeErr
	}
}

func (r *sourceDef) Protos(protos Protos) {
	r.protos = protos
}

func (r *sourceDef) handle(ctx context.Context, proto Proto, cmd Command) error {
	switch cmd {
	case CmdSpec:
		return r.Spec(ctx, proto)
	case CmdCheck:
		return r.Check(ctx, proto)
	case CmdDiscover:
		return r.Run(ctx, proto, false)
	case CmdRead:
		return r.Run(ctx, proto, true)
	default:
		return fmt.Errorf("invalid path")
	}
}

func NewSource(config interface{}) *sourceDef {
	return &sourceDef{config: config}
}

func (r *sourceDef) Documentation(links ...string) *sourceDef {
	r.docs = append(r.docs, links...)
	return r
}

func (r *sourceDef) Version(version string) *sourceDef {
	r.version = version
	return r
}

func (r *sourceDef) Notes(links ...string) *sourceDef {
	r.notes = append(r.notes, links...)
	return r
}

func (r *sourceDef) HttpRunner(runner HttpRunner) *sourceDef {
	r.httpRunner = runner
	return r
}

func (r *sourceDef) GeneralStream(schema SchemaBuilder, runner ...GeneralRunner) *sourceDef {
	return r
}

func (r *sourceDef) FsStream(schema SchemaBuilder, runner ...FsRunner) *sourceDef {
	return r
}

func (r *sourceDef) DbStream(schema SchemaBuilder, runner ...DbRunner) *sourceDef {
	return r
}

func (r *sourceDef) HttpStream(schema SchemaBuilder, runner ...HttpRunner) *sourceDef {
	r.incremental = r.incremental || schema.Incremental
	var fn HttpRunner
	if len(runner) == 1 {
		fn = runner[0]
	}
	r.runners = append(r.runners, runnerTyp{schema: schema.Schema, httpRunner: fn})
	return r
}

func (r *sourceDef) Spec(ctx context.Context, proto Proto) error {
	return proto.EmitSpec(ConnectorSpecification{
		DocumentationURL:        strings.Join(r.docs, ","),
		SupportsIncremental:     r.incremental, // why is this important to share?
		ConnectionSpecification: jsonschema.New(r.config),
	})
}

func (r *sourceDef) Read(ctx context.Context, proto Proto) error {
	return r.Run(ctx, proto, true)
}

func (r *sourceDef) Run(ctx context.Context, proto Proto, sync bool) error {
	wg, ctx := errgroup.WithContext(ctx)
	for _, runner := range r.runners {
		runner := runner // copy
		wg.Go(func() (err error) {
			return run(ctx, proto, runner, sync)
		})
	}
	return wg.Wait()
}

func (r *sourceDef) Check(ctx context.Context, proto Proto) error {
	for _, runner := range r.runners {
		runCtx := newRunCtx(ctx, runner.schema, proto)
		if err := runner.httpRunner.Run(&validatorLoader{runContext: *runCtx}); err == validatorOK {
			return proto.EmitStatus(nil)
		} else if err != nil {
			return proto.EmitStatus(fmt.Errorf("validation failed: %s", err.Error()))
		}
	}
	return fmt.Errorf("validation failed: unexpected error")
}

func (r *sourceDef) Discover(ctx context.Context, proto Proto) error {
	return r.Run(ctx, proto, false)
}

func (r *sourceDef) Validate() error {
	for _, runner := range r.runners {
		if err := runner.schema.Validate(); err != nil {
			return err
		}
	}
	return nil
}
