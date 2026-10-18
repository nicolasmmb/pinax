package cli

import (
	"flag"
	"fmt"
	"io"
)

type Command struct {
	Name      string
	Once      bool
	StatusJob string
	Watch     bool
	Limit     int
	IndexPath string
}

func Parse(args []string, stderr io.Writer) (Command, error) {
	if len(args) == 0 {
		return Command{}, usage(stderr)
	}
	switch args[0] {
	case "serve":
		if len(args) != 1 {
			return Command{}, usage(stderr)
		}
		return Command{Name: "serve"}, nil
	case "worker":
		flags := flag.NewFlagSet("worker", flag.ContinueOnError)
		flags.SetOutput(stderr)
		once := flags.Bool("once", false, "process the queue and exit when it is empty")
		if err := flags.Parse(args[1:]); err != nil {
			return Command{}, err
		}
		if flags.NArg() != 0 {
			return Command{}, usage(stderr)
		}
		return Command{Name: "worker", Once: *once}, nil
	case "status":
		flags := flag.NewFlagSet("status", flag.ContinueOnError)
		flags.SetOutput(stderr)
		watch := flags.Bool("watch", false, "refresh the view every second")
		flags.BoolVar(watch, "w", false, "shorthand for --watch")
		limit := flags.Int("limit", 10, "number of recent jobs to show (1-50)")
		flags.IntVar(limit, "n", 10, "shorthand for --limit")
		if err := flags.Parse(args[1:]); err != nil {
			return Command{}, err
		}
		if flags.NArg() > 1 {
			return Command{}, usage(stderr)
		}
		if *limit < 1 || *limit > 50 {
			return Command{}, fmt.Errorf("limit must be between 1 and 50")
		}
		return Command{Name: "status", StatusJob: flags.Arg(0), Watch: *watch, Limit: *limit}, nil
	case "index":
		if len(args) != 2 {
			return Command{}, usage(stderr)
		}
		return Command{Name: "index", IndexPath: args[1]}, nil
	default:
		return Command{}, usage(stderr)
	}
}

func usage(stderr io.Writer) error {
	fmt.Fprintln(stderr, "usage: pinax serve | pinax worker [--once] | pinax status [--watch] [--limit N] [job_id] | pinax index <path>")
	return fmt.Errorf("invalid command")
}
