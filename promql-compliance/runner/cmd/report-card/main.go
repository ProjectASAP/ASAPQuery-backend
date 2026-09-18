package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ProjectASAP/ASAPQuery-backend/promql-compliance/runner"
)

type card struct {
	Cases  []caseCard `json:"cases"`
	Passed bool       `json:"passed"`
}
type caseCard struct {
	Dataset, Suite                         string
	Passed                                 bool
	Queries, ASAPQuery, PrometheusFallback int
}

func main() {
	dir := flag.String("reports-dir", "/tmp/asapquery-backend-promql-reports", "report directory")
	flag.Parse()
	files, err := filepath.Glob(filepath.Join(*dir, "*.json"))
	if err != nil {
		panic(err)
	}
	out := card{Passed: true}
	for _, file := range files {
		var r runner.Report
		raw, e := os.ReadFile(file)
		if e != nil {
			panic(e)
		}
		if e = json.Unmarshal(raw, &r); e != nil {
			panic(e)
		}
		c := caseCard{Dataset: r.Dataset, Suite: r.Suite, Passed: r.Passed, Queries: len(r.Queries)}
		for _, q := range r.Queries {
			for _, i := range q.Instant {
				if i.Responses.Backend.ServedBy != "" {
					c.ASAPQuery++
				} else {
					c.PrometheusFallback++
				}
			}
		}
		out.Cases = append(out.Cases, c)
		out.Passed = out.Passed && c.Passed
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	_ = os.WriteFile(filepath.Join(*dir, "summary.json"), b, 0644)
	f, _ := os.Create(filepath.Join(*dir, "summary.md"))
	defer f.Close()
	fmt.Fprintf(f, "# PromQL compliance report card\n\nOverall: **%t**\n\n| Dataset | Suite | Passed | Queries | ASAPQuery answers | Prometheus fallback |\n|---|---|---:|---:|---:|---:|\n", out.Passed)
	for _, c := range out.Cases {
		fmt.Fprintf(f, "| %s | %s | %t | %d | %d | %d |\n", c.Dataset, c.Suite, c.Passed, c.Queries, c.ASAPQuery, c.PrometheusFallback)
	}
}
