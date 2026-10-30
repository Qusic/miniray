// Command miniray is a small self-hosted proxy and reverse tunnel. It prepares
// a node config for every node of a hand-maintained topology, and runs one node
// config on each machine.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/Qusic/miniray/internal/config"
	"github.com/Qusic/miniray/internal/node"
)

const usage = `usage:
  miniray prepare <topology.json>   write a <topology>-<node>.json node config beside it for every node
  miniray run [-debug] <node.json>  run one node config; -debug logs every request too
`

func main() {
	var command string
	var args []string
	if len(os.Args) > 1 {
		command, args = os.Args[1], os.Args[2:]
	}
	flags := flag.NewFlagSet(command, flag.ExitOnError)
	flags.Usage = func() { fmt.Fprint(flags.Output(), usage) }
	var err error
	switch command {
	case "prepare":
		err = prepare(parseArg(flags, args))
	case "run":
		debug := flags.Bool("debug", false, "log every request too")
		path := parseArg(flags, args) // parses -debug, so before *debug
		err = run(path, *debug)
	default:
		flags.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "miniray %s: %v\n", command, err)
		os.Exit(1)
	}
}

// parseArg parses the flags in args and returns the one positional argument.
// Anything else gets the usage, as -h does.
func parseArg(flags *flag.FlagSet, args []string) string {
	flags.Parse(args)
	if flags.NArg() != 1 {
		flags.Usage()
		os.Exit(2)
	}
	return flags.Arg(0)
}

// prepare writes home-hub.json etc. next to home.json. LoadTopology validates
// every node first, so an invalid topology writes nothing. Configs of nodes
// dropped from the topology are left in place.
func prepare(path string) error {
	topology, err := config.LoadTopology(path)
	if err != nil {
		return err
	}
	base := strings.TrimSuffix(path, filepath.Ext(path))
	for _, cfg := range topology.Prepare() {
		file := base + "-" + string(cfg.Name) + ".json"
		if err := cfg.Save(file); err != nil {
			return err
		}
		fmt.Println(file)
	}
	return nil
}

// run serves one node config. Every log line of the process names the node.
// The log follows the state of the node, and debug adds the start and end of
// every request.
func run(path string, debug bool) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	slog.SetDefault(slog.Default().With("node", cfg.Name))
	slog.Info("starting", "version", version())
	return node.Run(cfg)
}

// version is the version Go recorded for the build: one that names the
// commit, such as v0.0.0-20060102150405-abcdef123456, or (devel) if none.
func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	return info.Main.Version
}
