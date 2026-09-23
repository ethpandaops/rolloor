// Command rolloor-ethpandaops is the hook program set for ethpandaops
// devnets. Link it under each program name in rolloor's hooks directory, or
// run it with the program name as the first argument. "teams" writes the
// teams file from the coredevs registry.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ethpandaops/rolloor/contrib/ethpandaops"
)

const teamsCommand = "teams"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := run(ctx, os.Args, os.Stdin, os.Stdout, os.Getenv)

	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdin io.Reader, out io.Writer, getenv func(string) string) int {
	name := filepath.Base(args[0])
	rest := args[1:]

	if _, known := ethpandaops.Programs[name]; !known {
		if len(rest) == 0 {
			fmt.Fprintf(out, "usage: %s <program>|teams; programs: %s\n", name, strings.Join(ethpandaops.Names(), ", "))

			return 2
		}

		name, rest = rest[0], rest[1:]
	}

	if name == teamsCommand {
		return teams(ctx, rest, out)
	}

	s, err := ethpandaops.SettingsFromEnv(getenv)
	if err != nil {
		fmt.Fprintln(out, err.Error())

		return 2
	}

	return ethpandaops.Run(ctx, ethpandaops.New(&s), name, stdin, out)
}

// pairs is a repeatable owner=value flag.
type pairs map[string][]string

func (p pairs) String() string { return fmt.Sprint(map[string][]string(p)) }

func (p pairs) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" || val == "" {
		return fmt.Errorf("want owner=value, got %q", v)
	}

	p[k] = append(p[k], val)

	return nil
}

func teams(ctx context.Context, args []string, out io.Writer) int {
	fs := flag.NewFlagSet(teamsCommand, flag.ContinueOnError)
	fs.SetOutput(out)

	api := fs.String("api", "https://coredevs.analytics.production.platform.ethpandaops.io", "coredevs registry base URL")
	file := fs.String("out", "/etc/rolloor/teams.yaml", "teams file to write")
	team := pairs{}
	member := pairs{}

	fs.Var(team, "team", "owner=coredevs-team-slug, repeatable")
	fs.Var(member, "member", "owner=handle, repeatable")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	req := &ethpandaops.TeamsRequest{API: *api, Out: *file, Teams: team, Members: member}

	n, err := ethpandaops.RefreshTeams(ctx, &http.Client{Timeout: 30 * time.Second}, req)
	if err != nil {
		fmt.Fprintf(out, "teams: %v; %s left as it was\n", err, *file)

		return 1
	}

	fmt.Fprintf(out, "teams: wrote %d owners to %s\n", n, *file)

	return 0
}
