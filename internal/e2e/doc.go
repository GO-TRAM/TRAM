// Package e2e tests complete pipelines end to end without external
// dependencies: streams are built by the core from the real source and stage
// factories, OTLP data is sent over HTTP, and capturing sinks record exactly
// which records arrive where.
package e2e
