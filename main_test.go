package main

import (
	"bytes"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/Qusic/miniray/internal/config"
)

const testTopology = `{
  "credential": {
    "tunnel": "wss://:tunnel-secret@hub.example.com/tunnel",
    "relay": "wss://:relay-secret@relay.example.com/relay"
  },
  "node": {
    "hub": { "ingress": { "tunnel": 8080 } },
    "relay": { "ingress": { "public": 8081 } }
  }
}`

func files(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestPrepareWritesEveryNode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "home.json")
	if err := os.WriteFile(path, []byte(testTopology), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepare(path); err != nil {
		t.Fatal(err)
	}
	if got, want := files(t, dir), []string{"home-hub.json", "home-relay.json", "home.json"}; !slices.Equal(got, want) {
		t.Fatalf("files = %q, want %q", got, want)
	}
	for _, name := range []string{"hub", "relay"} {
		file := filepath.Join(dir, "home-"+name+".json")
		if cfg, err := config.Load(file); err != nil || string(cfg.Name) != name {
			t.Errorf("%s: loaded %q, %v", file, cfg.Name, err)
		}
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			t.Errorf("%s: mode %v, want private", file, info.Mode().Perm())
		}
	}
}

func TestPrepareWritesNothingForInvalidTopology(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "home.json")
	// hub is valid and sorts first; relay's error must still prevent every write.
	invalid := strings.Replace(testTopology, `"public": 8081 }`, `"public": 8081 }, "forward": { "nas": ["2202:22"] }`, 1)
	if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepare(path); err == nil || !strings.Contains(err.Error(), "node relay") {
		t.Fatalf("prepare = %v, want relay error", err)
	}
	if got := files(t, dir); !slices.Equal(got, []string{"home.json"}) {
		t.Fatalf("files after failure = %q", got)
	}
}

// run logs the version as it starts, names the node in every log line, and
// with -debug adds debug messages.
func TestRunLogging(t *testing.T) {
	path := filepath.Join(t.TempDir(), "desk.json")
	if err := os.WriteFile(path, []byte(`{"name": "desk"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	logger, output := slog.Default(), log.Writer()
	var out bytes.Buffer
	log.SetOutput(&out)
	fresh := func() { // as a process starts
		slog.SetDefault(logger)
		slog.SetLogLoggerLevel(slog.LevelInfo)
	}
	t.Cleanup(func() {
		fresh()
		log.SetOutput(output)
	})
	for _, debug := range []bool{false, true} {
		fresh()
		out.Reset()
		if err := run(path, debug); err == nil || !strings.Contains(err.Error(), "nothing to run") {
			t.Fatalf("run = %v, want nothing to run", err)
		}
		slog.Debug("probe debug")
		slog.Info("probe info")
		got := out.String()
		if !strings.Contains(got, "INFO starting node=desk version=") {
			t.Errorf("run with debug %t: log %q does not give the version", debug, got)
		}
		if !strings.Contains(got, "probe info node=desk") {
			t.Errorf("run with debug %t: log %q does not name the node", debug, got)
		}
		if strings.Contains(got, "probe debug") != debug {
			t.Errorf("run with debug %t: log %q", debug, got)
		}
	}
}
