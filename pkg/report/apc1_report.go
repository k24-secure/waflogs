package report

import (
	"fmt"
	"kfzteile24/waflogs/pkg/aws"
	"kfzteile24/waflogs/pkg/query"
	"time"
)

type APC1ReportLoader struct {
	base *ReportLoader
}

func NewAPC1ReportLoader(a *aws.AthenaClient, waf query.WAF, t time.Time) *APC1ReportLoader {
	year, month, day := getDay(t)

	out := &APC1ReportLoader{
		base: NewReportLoader(a, waf, "apc1", year, month, day),
	}

	return out
}

func (r *APC1ReportLoader) Run() error {
	r.base.ensureOutDirExists()

	if err := r.CreateMaterializedView(); err != nil {
		return fmt.Errorf("creating materialized view for APC1: %s", err)
	}

	if err := r.LoadScrapedURLs(); err != nil {
		return fmt.Errorf("loading scraped URLs by APC1: %s", err)
	}

	if err := r.LoadScraperUserAgents(); err != nil {
		return fmt.Errorf("loading scraper User Agents from APC1: %s", err)
	}

	if err := r.LoadScrapedProducts(); err != nil {
		return fmt.Errorf("loading products scraped by APC1: %s", err)
	}

	return nil
}

func (r *APC1ReportLoader) CreateMaterializedView() error {
	fmt.Println("\n[+] Creating Parquet waflog view...")

	sql, err := query.CreateAPC1MaterializedView(
		r.base.Scope,
	)
	if err != nil {
		return fmt.Errorf("rendering sql: %s", err)
	}

	if err := r.base.RunQuery(sql, "create-materialized-view"); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	return nil
}

func (r *APC1ReportLoader) LoadScrapedURLs() error {
	fmt.Println("\n[+] Loading scaped URLs with request counts...")

	sql, err := query.GetAPC1URLs(
		r.base.Scope,
		100,
	)
	if err != nil {
		return fmt.Errorf("rendering sql: %s", err)
	}

	if err := r.base.RunQuery(sql, "scraped-urls"); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	return nil
}

func (r *APC1ReportLoader) LoadScraperUserAgents() error {
	fmt.Println("\n[+] Loading scaper User Agents with request counts and time window...")

	sql, err := query.GetAPC1UserAgents(
		r.base.Scope,
		1000,
	)
	if err != nil {
		return fmt.Errorf("rendering sql: %s", err)
	}

	if err := r.base.RunQuery(sql, "scraper-user-agents"); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	return nil
}

func (r *APC1ReportLoader) LoadScrapedProducts() error {
	fmt.Println("\n[+] Loading products scraped...")

	sql, err := query.GetAPC1ScrapedProducts(
		r.base.Scope,
		200000,
	)
	if err != nil {
		return fmt.Errorf("rendering sql: %s", err)
	}

	if err := r.base.RunQuery(sql, "scraper-user-agents"); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	return nil
}
