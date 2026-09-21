package examples_test

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func norm(s string) string { return strings.ReplaceAll(s, "\r\n", "\n") }

// TestExamplesProduceExpectedOutput runs every example with `go run` and
// compares stdout with its expected_output.txt.
func TestExamplesProduceExpectedOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go run for each example")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not on PATH")
	}
	for _, name := range []string{"quickstart", "restart", "skipretry", "filerepo", "demo"} {
		t.Run(name, func(t *testing.T) {
			want, err := os.ReadFile(name + "/expected_output.txt")
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(goBin, "run", "./"+name)
			var stderr strings.Builder
			cmd.Stderr = &stderr
			got, err := cmd.Output()
			if err != nil {
				t.Fatalf("go run: %v\n%s", err, stderr.String())
			}
			if norm(string(got)) != norm(string(want)) {
				t.Fatalf("output mismatch\n--- got\n%s\n--- want\n%s", got, want)
			}
		})
	}
}

// TestReadmeQuickstartMatchesExample keeps the README quick start identical to examples/quickstart/main.go.
func TestReadmeQuickstartMatchesExample(t *testing.T) {
	readme, err := os.ReadFile("../README.md")
	if err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile("quickstart/main.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile("(?s)## Quick start\n.*?```go\n(.*?)```").FindStringSubmatch(norm(string(readme)))
	if m == nil {
		t.Fatal("README has no Go block under '## Quick start'")
	}
	if strings.TrimSpace(m[1]) != strings.TrimSpace(norm(string(src))) {
		t.Fatal("README quick start differs from examples/quickstart/main.go; copy the file verbatim")
	}
}
