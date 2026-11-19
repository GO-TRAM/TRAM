// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"codeberg.org/eljoth/tram/internal/testutil/natstest"
)

const (
	scenarioPackage  = "./internal/scenario/"
	streamsBenchtime = "1x"

	pageFilePermissions = 0o644
	failureLines        = 30
	maxLineBytes        = 1 << 20
	percent             = 100
	spreadWarning       = 10
)

type options struct {
	out, input, raw             string
	count                       int
	scenarioTime, componentTime string
}

func main() {
	var opts options
	flag.StringVar(&opts.out, "out", "BENCHMARK.md", "page to update")
	flag.StringVar(&opts.input, "input", "", "use this saved benchmark output instead of running the benchmarks")
	flag.StringVar(&opts.raw, "raw", "", "save the raw benchmark output to this file")
	flag.IntVar(&opts.count, "count", 3, "runs per benchmark; the page shows medians")
	flag.StringVar(&opts.scenarioTime, "scenario-benchtime", "1s", "benchtime of the scenario benchmarks")
	flag.StringVar(&opts.componentTime, "component-benchtime", "300ms", "benchtime of the component benchmarks")
	flag.Parse()

	if err := runInterruptible(opts); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func runInterruptible(opts options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return run(ctx, opts)
}

func run(ctx context.Context, opts options) error {
	page, err := os.ReadFile(opts.out)
	if err != nil {
		return err
	}
	env := environment(ctx)

	output, err := benchmarkOutput(ctx, opts, env)
	if err != nil {
		return err
	}

	results, meta := parse(output)
	env.cpu = meta.cpu
	if meta.commit != "" {
		env.commit, env.date = meta.commit, meta.date
	}
	sections := map[string]string{
		"environment":           renderEnvironment(env, opts.count, opts.scenarioTime, opts.componentTime),
		"scenarios":             renderScenarios(results),
		"sizing":                renderSizing(results),
		"streams":               renderStreams(results),
		"streams-fleet":         renderStreamsFleet(results),
		"components-per-record": renderComponents(results, perRecordComponents(), true),
		"components-per-event":  renderComponents(results, perEventComponents(), false),
	}
	for name, content := range sections {
		if page, err = replaceSection(page, name, content); err != nil {
			return err
		}
	}
	//nolint:gosec // BENCHMARK.md is a repository file that everyone may read.
	if err = os.WriteFile(opts.out, page, pageFilePermissions); err != nil {
		return err
	}
	log.Printf("updated %s", opts.out)
	return nil
}

func benchmarkOutput(ctx context.Context, opts options, env env) ([]byte, error) {
	if opts.input != "" {
		return os.ReadFile(opts.input)
	}

	output := fmt.Appendf(nil, "%s%s\n%s%s\n", commitPrefix, env.commit, datePrefix, env.date)
	measured, err := runBenchmarks(ctx, opts.count, opts.scenarioTime, opts.componentTime)
	if err != nil {
		return nil, err
	}
	output = append(output, measured...)
	if opts.raw != "" {
		if err = os.WriteFile(opts.raw, output, pageFilePermissions); err != nil {
			return nil, err
		}
	}
	return output, nil
}

func runBenchmarks(ctx context.Context, count int, scenarioTime, componentTime string) ([]byte, error) {
	var out bytes.Buffer
	log.Print("running scenario benchmarks")
	if err := goTest(ctx, &out, "Scenario", scenarioTime, count, scenarioPackage); err != nil {
		return nil, err
	}
	log.Print("running streams benchmarks")
	if err := goTest(ctx, &out, "Streams", streamsBenchtime, count, scenarioPackage); err != nil {
		return nil, err
	}

	packages, err := goList(ctx, "./internal/...")
	if err != nil {
		return nil, err
	}
	packages = slices.DeleteFunc(packages, func(p string) bool { return strings.HasSuffix(p, "/internal/scenario") })
	log.Print("running component benchmarks")
	if err = goTest(ctx, &out, ".", componentTime, count, packages...); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func goTest(ctx context.Context, out *bytes.Buffer, pattern, benchtime string, count int, packages ...string) error {
	args := append([]string{"test", "-p", "1", "-run", "^$", "-bench", pattern, "-benchmem",
		"-benchtime", benchtime, "-count", strconv.Itoa(count)}, packages...)
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Stdout = io.MultiWriter(&stdout, progress{})
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		return fmt.Errorf("go test: %w\n%s", err, strings.Join(lines[max(0, len(lines)-failureLines):], "\n"))
	}
	out.Write(stdout.Bytes())
	return nil
}

type progress struct{}

func (progress) Write(p []byte) (int, error) {
	for line := range strings.SplitSeq(string(p), "\n") {
		if after, ok := strings.CutPrefix(line, "pkg: "); ok {
			log.Print(after)
		}
	}
	return len(p), nil
}

func goList(ctx context.Context, pattern string) ([]string, error) {
	out, err := exec.CommandContext(ctx, "go", "list", pattern).Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w", err)
	}
	return strings.Fields(string(out)), nil
}

type env struct {
	commit, date, goVersion, cpu string
}

const (
	commitPrefix = "benchreport-commit: "
	datePrefix   = "benchreport-date: "
)

type metadata struct {
	commit, date, cpu string
}

func environment(ctx context.Context) env {
	commit := strings.TrimSpace(command(ctx, "git", "rev-parse", "--short", "HEAD"))
	if strings.TrimSpace(command(ctx, "git", "status", "--porcelain", "--untracked-files=no")) != "" {
		commit += " (with uncommitted changes)"
	}
	return env{
		commit:    commit,
		date:      time.Now().Format(time.DateOnly),
		goVersion: strings.TrimSpace(command(ctx, "go", "env", "GOVERSION")),
	}
}

func command(ctx context.Context, name string, args ...string) string {
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return "unknown"
	}
	return string(out)
}

type result struct {
	metrics map[string]float64
	spread  float64
}

var benchLine = regexp.MustCompile(`^Benchmark(\S+?)(-\d+)?\s+\d+\s+(.*)$`)

func parse(output []byte) (map[string]result, metadata) {
	runs := map[string][]map[string]float64{}
	pkg, meta := "", metadata{}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, maxLineBytes), maxLineBytes)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "pkg: "):
			pkg = strings.TrimPrefix(strings.TrimPrefix(line, "pkg: "), "codeberg.org/eljoth/tram/")
		case strings.HasPrefix(line, "cpu: "):
			meta.cpu = strings.TrimPrefix(line, "cpu: ")
		case strings.HasPrefix(line, commitPrefix):
			meta.commit = strings.TrimPrefix(line, commitPrefix)
		case strings.HasPrefix(line, datePrefix):
			meta.date = strings.TrimPrefix(line, datePrefix)
		default:
			if name, metrics, ok := parseBenchLine(line); ok {
				key := pkg + "|" + name
				runs[key] = append(runs[key], metrics)
			}
		}
	}

	results := map[string]result{}
	for key, rs := range runs {
		results[key] = summarize(rs)
	}
	return results, meta
}

func parseBenchLine(line string) (string, map[string]float64, bool) {
	m := benchLine.FindStringSubmatch(line)
	if m == nil {
		return "", nil, false
	}
	fields := strings.Fields(m[3])
	metrics := map[string]float64{}
	for i := 0; i+1 < len(fields); i += 2 {
		if v, err := strconv.ParseFloat(fields[i], 64); err == nil {
			metrics[fields[i+1]] = v
		}
	}
	_, ok := metrics["ns/op"]
	return m[1], metrics, ok
}

func summarize(runs []map[string]float64) result {
	r := result{metrics: map[string]float64{}}
	for unit := range runs[0] {
		var values []float64
		for _, run := range runs {
			values = append(values, run[unit])
		}
		r.metrics[unit] = median(values)
	}
	var ns []float64
	for _, run := range runs {
		ns = append(ns, run["ns/op"])
	}
	r.spread = (slices.Max(ns) - slices.Min(ns)) / median(ns) * percent
	return r
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	sort.Float64s(sorted)
	middle := len(sorted) / 2 //nolint:mnd // Halving finds the middle.
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2 //nolint:mnd // The mean of the two middle values.
}

var recordsInName = regexp.MustCompile(`records=(\d+)`)

func recordsPerOp(name string) float64 {
	m := recordsInName.FindStringSubmatch(name)
	if m == nil {
		return 1
	}
	n, _ := strconv.ParseFloat(m[1], 64)
	return n
}

func renderEnvironment(e env, count int, scenarioTime, componentTime string) string {
	var b strings.Builder
	b.WriteString("| | |\n|---|---|\n")
	fmt.Fprintf(&b, "| Commit | `%s` |\n", e.commit)
	fmt.Fprintf(&b, "| Date | %s |\n", e.date)
	fmt.Fprintf(&b, "| CPU | %s |\n", or(e.cpu, "unknown"))
	fmt.Fprintf(&b, "| Go | %s |\n", e.goVersion)
	fmt.Fprintf(&b, "| NATS | `%s` in a local container |\n", natstest.Image)
	fmt.Fprintf(
		&b,
		"| Runs | %d per benchmark, medians shown; scenarios `-benchtime=%s`, streams `-benchtime=%s`, components `-benchtime=%s` |\n",
		count,
		scenarioTime,
		streamsBenchtime,
		componentTime,
	)
	return b.String()
}

func scenarioOrder() []struct{ name, title string } {
	return []struct{ name, title string }{
		{"otlp-nats", "OTLP → NATS"},
		{"otlp-batch-nats", "OTLP → batch → NATS"},
		{"otlp-split-nats", "OTLP → 2 × cel_filter → 2 × NATS"},
		{scenarioNATSFilterNATS, "NATS → cel_filter → NATS"},
	}
}

const scenarioPkg = "internal/scenario"

func renderScenarios(results map[string]result) string {
	var b strings.Builder
	for _, sc := range scenarioOrder() {
		var keys []string
		for key := range results {
			if strings.HasPrefix(key, scenarioPkg+"|Scenario/"+sc.name+"/") {
				keys = append(keys, key)
			}
		}
		if len(keys) == 0 {
			continue
		}
		sort.Slice(keys, func(i, j int) bool { return scenarioSortKey(keys[i]) < scenarioSortKey(keys[j]) })

		fmt.Fprintf(&b, "#### `%s`: %s\n\n", sc.name, sc.title)
		b.WriteString(
			"| Callers | Records per event | Records/s | Events/s | CPU per record | Records/s per core | p50 latency | p99 latency | Allocs per record | Spread |\n",
		)
		b.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
		for _, key := range keys {
			r := results[key]
			name := strings.TrimPrefix(key, scenarioPkg+"|")
			records := recordsPerOp(name)
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %.0f | %s |\n",
				param(name, "concurrency"), resourcesLabel(name),
				count(r.metrics["records/s"]), count(r.metrics["events/s"]),
				duration(r.metrics["cpu-ns/record"]), count(r.metrics["records/s/core"]),
				millis(r.metrics, "p50-ms"), millis(r.metrics, "p99-ms"),
				r.metrics[allocsPerOp]/records, spread(r.spread))
		}
		b.WriteString("\n")
	}
	if b.Len() == 0 {
		return "_No scenario results; the scenario benchmarks need Docker or Podman._\n"
	}
	return b.String()
}

func scenarioSortKey(key string) string {
	return fmt.Sprintf("%03s/%06s", param(key, "concurrency"), param(key, "records"))
}

func param(name, key string) string {
	m := regexp.MustCompile(key + `=(\d+)`).FindStringSubmatch(name)
	if m == nil {
		return ""
	}
	return m[1]
}

func resourcesLabel(name string) string {
	return fmt.Sprintf(
		"%s (%s %s)",
		param(name, "records"),
		param(name, "resources"),
		plural(param(name, "resources"), "service", "services"),
	)
}

func plural(n, one, many string) string {
	if n == "1" {
		return one
	}
	return many
}

func sizingRows() []struct{ label, name string } {
	return []struct{ label, name string }{
		{
			"Batched: OTLP → batch → NATS, 1,000 records per request, 8 callers",
			"Scenario/otlp-batch-nats/concurrency=8/records=1000/resources=10/teams=1",
		},
		{
			"Split: OTLP → 2 × cel_filter → 2 × NATS, 1,000 records per request, 8 callers",
			"Scenario/otlp-split-nats/concurrency=8/records=1000/resources=10/teams=2",
		},
		{
			"Forwarding: NATS → cel_filter → NATS, 1,000 records per message, 8 workers",
			"Scenario/nats-filter-nats/concurrency=8/records=1000/resources=10/teams=1",
		},
		{
			"Unbatched: OTLP → NATS, 1 record per request, 8 callers",
			"Scenario/otlp-nats/concurrency=8/records=1/resources=1/teams=1",
		},
	}
}

func renderSizing(results map[string]result) string {
	var b strings.Builder
	b.WriteString(
		"| Pipeline | CPU per record | Cores for 100 k records/s | Cores for 1 M records/s |\n|---|---|---|---|\n",
	)
	for _, row := range sizingRows() {
		r, ok := results[scenarioPkg+"|"+row.name]
		if !ok {
			fmt.Fprintf(&b, "| %s | n/a | n/a | n/a |\n", row.label)
			continue
		}
		cpu := r.metrics["cpu-ns/record"]
		fmt.Fprintf(&b, "| %s | %s | %.1f | %.1f |\n",
			row.label, duration(cpu), cpu*hundredThousand/billion, cpu*million/billion)
	}
	return b.String()
}

const scenarioNATSFilterNATS = "nats-filter-nats"

func streamCounts() []int {
	return []int{10, 100, 1000}
}

const fleetBaseStreams = 1000

const minStreamsForIdleCPU = 1000

func streamsShapes() []struct{ name, workers, title string } {
	return []struct{ name, workers, title string }{
		{scenarioNATSFilterNATS, "1", "NATS (1 worker) → cel_filter → NATS"},
		{scenarioNATSFilterNATS, "4", "NATS (4 workers) → cel_filter → NATS"},
		{"otlp-batch-nats", "0", "OTLP → batch → NATS"},
	}
}

func streamsKey(shape, workers string, streams int) string {
	return fmt.Sprintf("%s|Streams/%s/workers=%s/streams=%d", scenarioPkg, shape, workers, streams)
}

func renderStreams(results map[string]result) string {
	var b strings.Builder
	b.WriteString(
		"| Stream | Streams | Build | Heap | Goroutines | NATS connections | NATS messages/s, idle | Idle CPU of a core |\n",
	)
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	rows := 0
	for _, shape := range streamsShapes() {
		for _, n := range streamCounts() {
			r, ok := results[streamsKey(shape.name, shape.workers, n)]
			if !ok {
				continue
			}
			rows++
			m := r.metrics
			idleCPU := "–"
			if n >= minStreamsForIdleCPU {
				idleCPU = coreShare(m["idle-cpu-µs/s/stream"])
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %.0f KiB | %.1f | %.1f | %.2f | %s |\n",
				shape.title, streamCount(n),
				duration(m["build-µs/stream"]*thousand), m["heap-KiB/stream"], m["goroutines/stream"],
				m["nats-conns/stream"], m["nats-msgs/s/stream"], idleCPU)
		}
	}
	if rows == 0 {
		return "_No streams results; the streams benchmarks need Docker or Podman._\n"
	}
	return b.String()
}

func fleetSizes() []struct{ streams, replicas int } {
	return []struct{ streams, replicas int }{
		{100, 1}, {100, 10}, {1000, 1}, {1000, 3}, {1000, 10},
	}
}

func renderStreamsFleet(results map[string]result) string {
	var b strings.Builder
	b.WriteString(
		"| Stream | Streams | Replicas | Heap per replica | Idle CPU per replica, of a core | NATS connections | NATS messages/s, idle |\n",
	)
	b.WriteString("|---|---|---|---|---|---|---|\n")
	rows := 0
	for _, shape := range streamsShapes() {
		r, ok := results[streamsKey(shape.name, shape.workers, fleetBaseStreams)]
		if !ok {
			continue
		}
		m := r.metrics
		for _, size := range fleetSizes() {
			rows++
			n, replicas := float64(size.streams), float64(size.replicas)
			fmt.Fprintf(&b, "| %s | %s | %d | %.0f MiB | %s | %s | %s |\n",
				shape.title, streamCount(size.streams), size.replicas,
				m["heap-KiB/stream"]*n/kibibyte, coreShare(m["idle-cpu-µs/s/stream"]*n),
				count(m["nats-conns/stream"]*n*replicas), count(m["nats-msgs/s/stream"]*n*replicas))
		}
	}
	if rows == 0 {
		return "_No streams results; the streams benchmarks need Docker or Podman._\n"
	}
	return b.String()
}

func coreShare(microsPerSecond float64) string {
	return fmt.Sprintf("%.2g %%", microsPerSecond/million*percent)
}

func streamCount(n int) string {
	if n < thousand {
		return strconv.Itoa(n)
	}
	return fmt.Sprintf("%d,%03d", n/thousand, n%thousand)
}

type component struct{ label, pkg, name string }

const (
	pkgCore         = "internal/core"
	pkgDomain       = "internal/core/domain"
	pkgBrokerSink   = "internal/adapters/sink/broker"
	pkgBrokerSource = "internal/adapters/source/broker"
	pkgOTLPSource   = "internal/adapters/source/otlp"
	pkgSourceBase   = "internal/adapters/source/base"
	pkgBatch        = "internal/adapters/stage/batch"
	pkgCELFilter    = "internal/adapters/stage/celfilter"
	pkgCELValidator = "internal/adapters/stage/celvalidator"
)

func perRecordComponents() []component {
	return []component{
		{
			"Broker sink (NATS): group and publish, 10 subjects",
			pkgBrokerSink,
			"Notify/records=1000/resources=10/teams=10",
		},
		{
			"Broker sink (NATS): group and publish, 1 subject",
			pkgBrokerSink,
			"Notify/records=1000/resources=10/teams=1",
		},
		{
			"Serializing OTLP data and back (logs)",
			pkgDomain,
			"OTLPDataRoundTrip/logs/records=1000/resources=10/teams=1",
		},
		{"Broker source (NATS), 1 worker", pkgBrokerSource, "Consume/workers=1/records=1000/resources=10/teams=1"},
		{"Broker source (NATS), 8 workers", pkgBrokerSource, "Consume/workers=8/records=1000/resources=10/teams=1"},
		{
			"OTLP source: decoding a request",
			pkgOTLPSource,
			"HandleLogs/records=1000/resources=10/teams=1",
		},
		{
			"cel_filter, keeping half of the records",
			pkgCELFilter,
			"CELFilter/keep-half/records=1000/resources=10/teams=2",
		},
		{
			"cel_filter, keeping all records",
			pkgCELFilter,
			"CELFilter/keep-all/records=1000/resources=10/teams=2",
		},
		{
			"cel_validator, record-level expression",
			pkgCELValidator,
			"CELValidator/record-attribute/records=1000/resources=10/teams=1",
		},
		{
			"cel_validator, resource-level expression",
			pkgCELValidator,
			"CELValidator/resource-attribute/records=1000/resources=10/teams=1",
		},
		{
			"Record filter helper without CEL",
			pkgDomain,
			"Filter/logs/keep-half/records=1000/resources=10/teams=2",
		},
		{
			"batch, 8 concurrent callers",
			pkgBatch,
			"Batch/parallel-8-events/records=1000/resources=10/teams=1",
		},
		{"noop stage", pkgCore, "Pipeline/noop/records=1000/resources=10/teams=1"},
	}
}

func perEventComponents() []component {
	return []component{
		{
			"Broker sink (NATS): one record (waits for the acknowledgement)",
			pkgBrokerSink,
			"Notify/records=1/resources=1/teams=1",
		},
		{
			"OTLP source: request with one record",
			pkgOTLPSource,
			"HandleLogs/records=1/resources=1/teams=1",
		},
		{"batch, one caller", pkgBatch, "Batch/flush-per-event/records=1/resources=1/teams=1"},
		{"Passing an event to 1 child", pkgSourceBase, "NotifySubscribers/children=1"},
		{"Passing an event to 8 children", pkgSourceBase, "NotifySubscribers/children=8"},
		{"Each additional noop stage", pkgCore, "Pipeline/noop/records=1/resources=1/teams=1"},
	}
}

func renderComponents(results map[string]result, components []component, perRecord bool) string {
	var b strings.Builder
	if perRecord {
		b.WriteString("| Component | Per record | Allocs per record | Records/s | Spread |\n|---|---|---|---|---|\n")
	} else {
		b.WriteString("| Component | Per event | Allocs per event | Spread |\n|---|---|---|---|\n")
	}
	for _, c := range components {
		r, ok := results[c.pkg+"|"+c.name]
		if !ok {
			if perRecord {
				fmt.Fprintf(&b, "| %s | n/a | n/a | n/a | |\n", c.label)
			} else {
				fmt.Fprintf(&b, "| %s | n/a | n/a | |\n", c.label)
			}
			continue
		}
		ns, allocs := componentCost(results, c, r)
		if perRecord {
			records := recordsPerOp(c.name)
			fmt.Fprintf(
				&b,
				"| %s | %s | %.1f | %s | %s |\n",
				c.label,
				duration(ns/records),
				allocs/records,
				count(records*billion/ns),
				spread(r.spread),
			)
		} else {
			fmt.Fprintf(&b, "| %s | %s | %.0f | %s |\n", c.label, duration(ns), allocs, spread(r.spread))
		}
	}
	return b.String()
}

// componentCost returns the time and allocations of a component. The noop
// stage is measured as a pipeline, so the cost of the direct pipeline is
// subtracted from it.
func componentCost(results map[string]result, c component, r result) (float64, float64) {
	ns, allocs := r.metrics["ns/op"], r.metrics[allocsPerOp]
	if c.name != "Pipeline/noop/records=1/resources=1/teams=1" {
		return ns, allocs
	}
	if direct, found := results[c.pkg+"|Pipeline/direct/records=1/resources=1/teams=1"]; found {
		ns -= direct.metrics["ns/op"]
		allocs -= direct.metrics[allocsPerOp]
	}
	return ns, allocs
}

func replaceSection(page []byte, name, content string) ([]byte, error) {
	begin := fmt.Sprintf("<!-- BEGIN benchreport:%s -->", name)
	end := fmt.Sprintf("<!-- END benchreport:%s -->", name)
	s := string(page)
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < i {
		return nil, fmt.Errorf("section %q not found; add %s and %s to the page", name, begin, end)
	}
	return []byte(s[:i+len(begin)] + "\n" + content + s[j:]), nil
}

const (
	allocsPerOp = "allocs/op"

	kibibyte        = 1024
	thousand        = 1e3
	tenThousand     = 1e4
	hundredThousand = 1e5
	million         = 1e6
	billion         = 1e9
)

func duration(ns float64) string {
	switch {
	case ns >= million:
		return fmt.Sprintf("%.1f ms", ns/million)
	case ns >= thousand:
		return fmt.Sprintf("%.2f µs", ns/thousand)
	default:
		return fmt.Sprintf("%.1f ns", ns)
	}
}

func count(x float64) string {
	switch {
	case x >= billion:
		return fmt.Sprintf("%.2f G", x/billion)
	case x >= million:
		return fmt.Sprintf("%.2f M", x/million)
	case x >= tenThousand:
		return fmt.Sprintf("%.0f k", x/thousand)
	case x >= thousand:
		return fmt.Sprintf("%.1f k", x/thousand)
	default:
		return fmt.Sprintf("%.0f", x)
	}
}

func millis(metrics map[string]float64, unit string) string {
	v, ok := metrics[unit]
	if !ok {
		return "–"
	}
	return fmt.Sprintf("%.2f ms", v)
}

func spread(percentage float64) string {
	if percentage > spreadWarning {
		return fmt.Sprintf("±%.0f%% ⚠️", percentage)
	}
	return fmt.Sprintf("±%.0f%%", percentage)
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
