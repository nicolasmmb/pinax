package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	mcpadapter "github.com/nicolasmmb/pinax/internal/adapter/mcp"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/config"
)

type Runtime struct {
	Ingest *ingest.Service
	Query  *query.Service
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer, runtime Runtime) error {
	if len(args) == 0 {
		return usage(stderr)
	}
	switch args[0] {
	case "serve":
		if len(args) != 1 {
			return usage(stderr)
		}
		server := mcpadapter.NewServer(runtime.Ingest, runtime.Query)
		return server.Run(ctx, &mcp.StdioTransport{})
	default:
		return usage(stderr)
	}
}

func Main() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) != 2 || os.Args[1] != "serve" {
		_ = usage(os.Stderr)
		return 2
	}
	cfg, err := config.Load()
	if err != nil {
		return reportError(os.Stderr, err)
	}
	runtime, closeRuntime, err := newRuntime(ctx, cfg)
	if err != nil {
		return reportError(os.Stderr, err)
	}
	defer closeRuntime()
	err = Run(ctx, os.Args[1:], os.Stdout, os.Stderr, runtime)
	if err != nil {
		return reportError(os.Stderr, err)
	}
	return 0
}

func reportError(stderr io.Writer, err error) int {
	fmt.Fprintln(stderr, err)
	return 1
}

func usage(stderr io.Writer) error {
	fmt.Fprintln(stderr, "usage: pinax serve")
	return fmt.Errorf("invalid command")
}
