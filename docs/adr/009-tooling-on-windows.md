# ADR-009: Developer tooling on native Windows

Status: accepted

## Context
The plan targets WSL2, but the first developer machine also builds natively on Windows.

## Decision
- `sqlc` runs from its pinned Docker image (`sqlc/sqlc:1.31.1`), because it needs cgo, which native Windows lacks.
- `make test` adds `-race` only where cgo exists (Linux, WSL2, CI). CI always runs with `-race`.
- Everything else (buf, protoc plugins, golangci-lint, goose, govulncheck) installs with `go install` at pinned versions (`make tools`).
