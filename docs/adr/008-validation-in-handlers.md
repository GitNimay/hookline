# ADR-008: Request validation in handlers, not a separate interceptor

Status: accepted

## Decision
Validation lives in each handler and returns `domain.FieldError`, which the error-mapping interceptor turns into `InvalidArgument` with `BadRequest` field violations. We do not add protovalidate.

## Why
protovalidate brings a CEL runtime for rules that are a few lines of Go each, and several checks need the database (the event type catalogue). Error-to-status mapping stays in one place (`server.ToStatus`).
