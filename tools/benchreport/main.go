// Command benchreport runs Tram's benchmarks and writes the results into the
// generated sections of BENCHMARK.md.
//
//	go run ./tools/benchreport
//
// The NATS benchmarks need Docker or Podman; see CONTRIBUTING.md. Sections are
// delimited by <!-- BEGIN benchreport:NAME --> and <!-- END benchreport:NAME -->
// markers; everything outside them is left unchanged.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"codeberg.org/eljoth/tram/internal/testutil/natstest"
)

const scenarioPackage = "./internal/scenario/"

func main() {
	out := flag.String("out", "BENCHMARK.md", "page to update")
	input := flag.String("input", "", "use this saved benchmark output instead of running the benchmarks")
	raw := flag.String("raw", "", "save the raw benchmark output to this file")
	count := flag.Int("count", 3, "runs per benchmark; the page shows medians")
	scenarioTime := flag.String("scenario-benchtime", "1s", "benchtime of the scenario benchmarks")
	componentTime := flag.String("component-benchtime", "300ms", "benchtime of the component benchmarks")
	flag.Parse()

	page, err := os.ReadFile(*out)
	if err != nil {
		log.Fatal(err)
	}
	env := environment()

	var output []byte
	if *input != "" {
		if output, err = os.ReadFile(*input); err != nil {
			log.Fatal(err)
		}
	} else {
		// The commit and date are stored with the output, so pages
		// regenerated from it later still show when it was measured.
		output = fmt.Appendf(nil, "%s%s\n%s%s\n", commitPrefix, env.commit, datePrefix, env.date)
		output = append(output, runBenchmarks(*count, *scenarioTime, *componentTime)...)
		if *raw != "" {
			if err := os.WriteFile(*raw, output, 0o644); err != nil {
				log.Fatal(err)
			}
		}
	}

	results, meta := parse(output)
	env.cpu = meta.cpu
	if meta.commit != "" {
		env.commit, env.date = meta.commit, meta.date
	}
	sections := map[string]string{
		"environment":           renderEnvironment(env, *count, *scenarioTime, *componentTime),
		"scenarios":             renderScenarios(results),
		"sizing":                renderSizing(results),
		"components-per-record": renderComponents(results, perRecordComponents, true),
		"components-per-event":  renderComponents(results, perEventComponents, false),
	}
	for name, content := range sections {
		if page, err = replaceSection(page, name, content); err != nil {
			log.Fatal(err)
		}
	}
	if err := os.WriteFile(*out, page, 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("updated %s", *out)
}

// runBenchmarks runs the scenario and component benchmarks one package at a
// time; parallel packages would distort each other's results.
func runBenchmarks(count int, scenarioTime, componentTime string) []byte {
	var out bytes.Buffer
	log.Print("running scenario benchmarks")
	goTest(&out, "Scenario", scenarioTime, count, scenarioPackage)

	packages := goList("./internal/...")
	packages = slices.DeleteFunc(packages, func(p string) bool { return strings.HasSuffix(p, "/internal/scenario") })
	log.Print("running component benchmarks")
	goTest(&out, ".", componentTime, count, packages...)
	return out.Bytes()
}

func goTest(out *bytes.Buffer, pattern, benchtime string, count int, packages ...string) {
	args := append([]string{"test", "-p", "1", "-run", "^$", "-bench", pattern, "-benchmem",
		"-benchtime", benchtime, "-count", strconv.Itoa(count)}, packages...)
	var stdout bytes.Buffer
	cmd := exec.Command("go", args...)
	cmd.Stdout = io.MultiWriter(&stdout, progress{})
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		// go test reports failures on stdout, which is otherwise only parsed.
		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		log.Fatalf("go test: %v\n%s", err, strings.Join(lines[max(0, len(lines)-30):], "\n"))
	}
	out.Write(stdout.Bytes())
}

// progress prints the package currently being benchmarked.
type progress struct{}

func (progress) Write(p []byte) (int, error) {
	for _, line := range strings.Split(string(p), "\n") {
		if strings.HasPrefix(line, "pkg: ") {
			log.Print(strings.TrimPrefix(line, "pkg: "))
		}
	}
	return len(p), nil
}

func goList(pattern string) []string {
	out, err := exec.Command("go", "list", pattern).Output()
	if err != nil {
		log.Fatalf("go list: %v", err)
	}
	return strings.Fields(string(out))
}

type env struct {
	commit, date, goVersion, cpu string
}

const (
	commitPrefix = "benchreport-commit: "
	datePrefix   = "benchreport-date: "
)

// metadata is read from the benchmark output.
type metadata struct {
	commit, date, cpu string
}

func environment() env {
	commit := strings.TrimSpace(command("git", "rev-parse", "--short", "HEAD"))
	if strings.TrimSpace(command("git", "status", "--porcelain", "--untracked-files=no")) != "" {
		commit += " (with uncommitted changes)"
	}
	return env{
		commit:    commit,
		date:      time.Now().Format("2006-01-02"),
		goVersion: strings.TrimSpace(command("go", "env", "GOVERSION")),
	}
}

func command(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return "unknown"
	}
	return string(out)
}

// result holds the medians of all runs of one benchmark.
type result struct {
	metrics map[string]float64
	// spread is the range of ns/op across runs relative to the median.
	spread float64
}

var benchLine = regexp.MustCompile(`^Benchmark(\S+?)(-\d+)?\s+\d+\s+(.*)$`)

// parse reads `go test -bench` output. Results are keyed by package path
// relative to the module and benchmark name, e.g.
// "internal/core|Pipeline/noop/records=1/resources=1/teams=1".
func parse(output []byte) (map[string]result, metadata) {
	runs := map[string][]map[string]float64{}
	pkg, meta := "", metadata{}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
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
			m := benchLine.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			fields := strings.Fields(m[3])
			metrics := map[string]float64{}
			for i := 0; i+1 < len(fields); i += 2 {
				if v, err := strconv.ParseFloat(fields[i], 64); err == nil {
					metrics[fields[i+1]] = v
				}
			}
			if _, ok := metrics["ns/op"]; ok {
				key := pkg + "|" + m[1]
				runs[key] = append(runs[key], metrics)
			}
		}
	}

	results := map[string]result{}
	for key, rs := range runs {
		r := result{metrics: map[string]float64{}}
		for unit := range rs[0] {
			var values []float64
			for _, run := range rs {
				values = append(values, run[unit])
			}
			r.metrics[unit] = median(values)
		}
		var ns []float64
		for _, run := range rs {
			ns = append(ns, run["ns/op"])
		}
		r.spread = (slices.Max(ns) - slices.Min(ns)) / median(ns) * 100
		results[key] = r
	}
	return results, meta
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
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
	fmt.Fprintf(&b, "| Runs | %d per benchmark, medians shown; scenarios `-benchtime=%s`, components `-benchtime=%s` |\n", count, scenarioTime, componentTime)
	return b.String()
}

var scenarioOrder = []struct{ name, title string }{
	{"otlp-nats", "OTLP → NATS"},
	{"otlp-batch-nats", "OTLP → batch → NATS"},
	{"otlp-split-nats", "OTLP → 2 × cel_filter → 2 × NATS"},
	{"nats-filter-nats", "NATS → cel_filter → NATS"},
}

const scenarioPkg = "internal/scenario"

func renderScenarios(results map[string]result) string {
	var b strings.Builder
	for _, sc := range scenarioOrder {
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
		b.WriteString("| Callers | Records per event | Records/s | Events/s | CPU per record | Records/s per core | p50 latency | p99 latency | Allocs per record | Spread |\n")
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
				r.metrics["allocs/op"]/records, spread(r.spread))
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
	return fmt.Sprintf("%s (%s %s)", param(name, "records"), param(name, "resources"), plural(param(name, "resources"), "service", "services"))
}

func plural(n, one, many string) string {
	if n == "1" {
		return one
	}
	return many
}

// sizingRows are the scenarios the sizing table is based on.
var sizingRows = []struct{ label, name string }{
	{"Batched: OTLP → batch → NATS, 1,000 records per request, 8 callers", "Scenario/otlp-batch-nats/concurrency=8/records=1000/resources=10/teams=1"},
	{"Split: OTLP → 2 × cel_filter → 2 × NATS, 1,000 records per request, 8 callers", "Scenario/otlp-split-nats/concurrency=8/records=1000/resources=10/teams=2"},
	{"Forwarding: NATS → cel_filter → NATS, 1,000 records per message, 8 workers", "Scenario/nats-filter-nats/concurrency=8/records=1000/resources=10/teams=1"},
	{"Unbatched: OTLP → NATS, 1 record per request, 8 callers", "Scenario/otlp-nats/concurrency=8/records=1/resources=1/teams=1"},
}

func renderSizing(results map[string]result) string {
	var b strings.Builder
	b.WriteString("| Pipeline | CPU per record | Cores for 100 k records/s | Cores for 1 M records/s |\n|---|---|---|---|\n")
	for _, row := range sizingRows {
		r, ok := results[scenarioPkg+"|"+row.name]
		if !ok {
			fmt.Fprintf(&b, "| %s | n/a | n/a | n/a |\n", row.label)
			continue
		}
		cpu := r.metrics["cpu-ns/record"]
		fmt.Fprintf(&b, "| %s | %s | %.1f | %.1f |\n", row.label, duration(cpu), cpu*1e5/1e9, cpu*1e6/1e9)
	}
	return b.String()
}

type component struct{ label, pkg, name string }

// perRecordComponents are measured with batches of 1,000 records from 10
// services, so their cost is shown per record.
var perRecordComponents = []component{
	{"NATS sink: group and publish, 10 subjects", "internal/adapters/sink/nats", "Notify/records=1000/resources=10/teams=10"},
	{"NATS sink: group and publish, 1 subject", "internal/adapters/sink/nats", "Notify/records=1000/resources=10/teams=1"},
	{"NATS sink: grouping by subject only", "internal/adapters/sink/nats", "GroupEventsBySubjects/records=1000/resources=10/teams=1"},
	{"Serializing for NATS and back (logs)", "internal/core/domain", "OTLPDataRoundTrip/logs/records=1000/resources=10/teams=1"},
	{"NATS source, 1 worker", "internal/adapters/source/nats", "Consume/workers=1/records=1000/resources=10/teams=1"},
	{"NATS source, 8 workers", "internal/adapters/source/nats", "Consume/workers=8/records=1000/resources=10/teams=1"},
	{"OTLP source: decoding a request", "internal/adapters/source/otlp", "HandleLogs/records=1000/resources=10/teams=1"},
	{"cel_filter, keeping half of the records", "internal/adapters/stage/celfilter", "CELFilter/keep-half/records=1000/resources=10/teams=2"},
	{"cel_filter, keeping all records", "internal/adapters/stage/celfilter", "CELFilter/keep-all/records=1000/resources=10/teams=2"},
	{"cel_validator, record-level expression", "internal/adapters/stage/celvalidator", "CELValidator/record-attribute/records=1000/resources=10/teams=1"},
	{"cel_validator, resource-level expression", "internal/adapters/stage/celvalidator", "CELValidator/resource-attribute/records=1000/resources=10/teams=1"},
	{"Record filter helper without CEL", "internal/core/domain", "Filter/logs/keep-half/records=1000/resources=10/teams=2"},
	{"batch, 8 concurrent callers", "internal/adapters/stage/batch", "Batch/parallel-8-events/records=1000/resources=10/teams=1"},
	{"noop stage", "internal/core", "Pipeline/noop/records=1000/resources=10/teams=1"},
}

// perEventComponents do not depend on the payload size.
var perEventComponents = []component{
	{"NATS sink: one record (waits for the acknowledgement)", "internal/adapters/sink/nats", "Notify/records=1/resources=1/teams=1"},
	{"OTLP source: request with one record", "internal/adapters/source/otlp", "HandleLogs/records=1/resources=1/teams=1"},
	{"batch, one caller", "internal/adapters/stage/batch", "Batch/flush-per-event/records=1/resources=1/teams=1"},
	{"Passing an event to 1 child", "internal/adapters/source/base", "NotifySubscribers/children=1"},
	{"Passing an event to 8 children", "internal/adapters/source/base", "NotifySubscribers/children=8"},
	{"Each additional noop stage", "internal/core", "Pipeline/noop/records=1/resources=1/teams=1"},
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
		ns, allocs := r.metrics["ns/op"], r.metrics["allocs/op"]
		if c.name == "Pipeline/noop/records=1/resources=1/teams=1" {
			// The noop scenario includes the source's own hop; subtract it.
			if direct, ok := results[c.pkg+"|Pipeline/direct/records=1/resources=1/teams=1"]; ok {
				ns -= direct.metrics["ns/op"]
				allocs -= direct.metrics["allocs/op"]
			}
		}
		if perRecord {
			records := recordsPerOp(c.name)
			fmt.Fprintf(&b, "| %s | %s | %.1f | %s | %s |\n", c.label, duration(ns/records), allocs/records, count(records*1e9/ns), spread(r.spread))
		} else {
			fmt.Fprintf(&b, "| %s | %s | %.0f | %s |\n", c.label, duration(ns), allocs, spread(r.spread))
		}
	}
	return b.String()
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

func duration(ns float64) string {
	switch {
	case ns >= 1e6:
		return fmt.Sprintf("%.1f ms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2f µs", ns/1e3)
	default:
		return fmt.Sprintf("%.1f ns", ns)
	}
}

func count(x float64) string {
	switch {
	case x >= 1e9:
		return fmt.Sprintf("%.2f G", x/1e9)
	case x >= 1e6:
		return fmt.Sprintf("%.2f M", x/1e6)
	case x >= 1e4:
		return fmt.Sprintf("%.0f k", x/1e3)
	case x >= 1e3:
		return fmt.Sprintf("%.1f k", x/1e3)
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

// spread flags results whose runs differ by more than 10%.
func spread(percent float64) string {
	if percent > 10 {
		return fmt.Sprintf("±%.0f%% ⚠️", percent)
	}
	return fmt.Sprintf("±%.0f%%", percent)
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
