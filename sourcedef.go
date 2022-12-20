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
	incremental      bool
	config           interface{}
	runners          runners
	protos           Protos
	sharedHttpRunner HttpRunner
	manualRunner     ManualRunner
	notes            []string
	docs             []string
	version          string
	concurrency      int
}

func (r *sourceDef) Handle(ctx context.Context, cmd Command, writer io.Writer, rd io.Reader, protos Protos) error {
	proto, err := Open(rd, writer, cmd, protos)
	if err != nil {
		return err
	}

	err = r.handleCmd(ctx, proto, cmd)
	closeErr := proto.Close()

	proto.Close()
	if err != nil {
		return err
	} else {
		return closeErr
	}
}

func (r *sourceDef) Protos(protos Protos) {
	r.protos = protos
}

func (r *sourceDef) handleCmd(ctx context.Context, proto Proto, cmd Command) error {
	switch cmd {
	case CmdSpec:
		return r.Spec(ctx, proto)
	case CmdCheck:
		return r.Check(ctx, proto)
	case CmdDiscover:
		return r.Discover(ctx, proto)
	case CmdRead:
		return r.Run(ctx, proto, true)
	default:
		return fmt.Errorf("invalid path")
	}
}

func NewSource(config interface{}) *sourceDef {
	return &sourceDef{config: config, concurrency: 1}
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

// the streams are interleaved
func (r *sourceDef) Interleaved() *sourceDef {
	return r
}

func (r *sourceDef) HttpRunner(runner HttpRunner) *sourceDef {
	r.sharedHttpRunner = runner
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

func (r *sourceDef) ManualRunner(runner ManualRunner) *sourceDef {
	r.manualRunner = runner
	return r
}

func (r *sourceDef) HttpStream(schema SchemaBuilder, runner ...HttpRunner) *sourceDef {
	r.incremental = r.incremental || schema.Incremental
	var fn HttpRunner
	if len(runner) == 1 {
		fn = runner[0]
	} else {
		fn = r.sharedHttpRunner
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

func (r *sourceDef) Check(ctx context.Context, proto Proto) error {
	for _, runner := range r.runners {
		stPr, err := proto.Open(runner.schema)
		if err != nil {
			return err
		}
		runCtx := newHTTPRunCtx(ctx, runner.schema, stPr)
		if err := runner.httpRunner.Run(&validatorLoader{httpRunContext: *runCtx}); err == validatorOK {
			return proto.EmitStatus(nil)
		} else if err != nil {
			return proto.EmitStatus(fmt.Errorf("validation failed: %s", err.Error()))
		}
	}
	return fmt.Errorf("validation failed: unexpected error")
}

// Emit scehmas by calling run without sync
func (r *sourceDef) Discover(ctx context.Context, proto Proto) error {
	return r.Run(ctx, proto, false)
}

func (r *sourceDef) Read(ctx context.Context, proto Proto) error {
	return r.Run(ctx, proto, true)
}

type throttler chan struct{}

func (t *throttler) Wrap(f func() error) func() error {
	return func() error {
		*t <- struct{}{}
		err := f()
		<-*t
		return err
	}
}

func (r *sourceDef) Run(ctx context.Context, proto Proto, sync bool) error {
	wg, ctx := errgroup.WithContext(ctx)

	var t = make(throttler, r.concurrency)

	for _, runner := range r.runners {
		runner := runner // copy
		wg.Go(t.Wrap(func() error {
			return run(ctx, proto, runner, sync)
		}))
	}

	if r.manualRunner != nil {
		c := &manualCtx{p: proto}
		wg.Go(t.Wrap(func() error {
			if err := r.manualRunner.Run(c); err != nil {
				return err
			}
			return c.Close()
		}))
	}
	return wg.Wait()
}

func (r *sourceDef) Validate() error {
	for _, runner := range r.runners {
		if err := runner.schema.Validate(); err != nil {
			return err
		}
	}
	return nil
}
