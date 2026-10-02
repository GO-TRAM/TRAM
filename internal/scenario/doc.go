// Package scenario benchmarks complete pipelines end to end: streams are built
// by the core from the real component factories, OTLP data arrives over HTTP,
// and NATS runs in a container. The benchmarks report throughput, CPU time
// per record and latency; tools/benchreport turns them into BENCHMARK.md.
package scenario
