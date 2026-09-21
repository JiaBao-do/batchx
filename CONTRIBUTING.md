# Contributing

Requires Go 1.24+. Enable the pre-push hook once per clone:

    git config core.hooksPath .githooks

The hook runs gofmt, `go mod tidy` (no diff), `go vet`, `go test -race -shuffle=on`, `go build` and
golangci-lint (if installed). Never bypass it. New behavior needs a test; a bug fix needs a regression test.
Use Conventional Commits and update CHANGELOG.md.
