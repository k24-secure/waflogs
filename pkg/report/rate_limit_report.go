package report

import (
	"fmt"
	"kfzteile24/waflogs/pkg/aws"
	"kfzteile24/waflogs/pkg/query"
	"strings"
	"time"
)

type RateLimitReportLoader struct {
	base *ReportLoader
}

func NewRateLimitReportLoader(a *aws.AthenaClient, waf query.WAF, t time.Time) *RateLimitReportLoader {
	year, month, day := getDay(t)

	out := &RateLimitReportLoader{
		base: NewReportLoader(a, waf, "rate-limit-report", year, month, day),
	}

	return out
}

func (r *RateLimitReportLoader) Run() error {
	r.base.ensureOutDirExists()

	if err := r.LoadIpsBlockedByRateLimit(); err != nil {
		return fmt.Errorf("loading requests per IP blocked by rate limit: %s", err)
	}

	if err := r.LoadUserAgentsBlockedByRateLimit(); err != nil {
		return fmt.Errorf("loading requests per User-Agent blocked by rate limit: %s", err)
	}

	if err := r.LoadFastestIPsNotBlackOrWhitelisted(); err != nil {
		return fmt.Errorf("loading ips with fastest requests: %s", err)
	}

	if err := r.LoadFastestBotUserAgentsNotBlackOrWhitelisted(); err != nil {
		return fmt.Errorf("loading ips with fastest requests: %s", err)
	}

	return nil
}

func (r *RateLimitReportLoader) LoadIpsBlockedByRateLimit() error {
	fmt.Println("\n[+] Loading requests per IP blocked by rate limit...")

	sql, err := query.GetRequestsBlockedBy(
		r.base.Scope,
		query.IdentityColumnsIP,
		[]query.TerminatingRule{query.TerminatingRuleRateLimit},
		1000,
	)
	if err != nil {
		return fmt.Errorf("rendering sql: %s", err)
	}

	if err := r.base.RunQuery(sql, "ips-blocked-by-rate-limit"); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	return nil
}

func (r *RateLimitReportLoader) LoadUserAgentsBlockedByRateLimit() error {
	fmt.Println("\n[+] Loading requests per User-Agent blocked by rate limit...")

	sql, err := query.GetRequestsBlockedBy(
		r.base.Scope,
		query.IdentityColumnsUserAgent,
		[]query.TerminatingRule{query.TerminatingRuleRateLimit},
		1000,
	)
	if err != nil {
		return fmt.Errorf("rendering sql: %s", err)
	}

	if err := r.base.RunQuery(sql, "user-agents-blocked-by-rate-limit"); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	return nil
}

func (r *RateLimitReportLoader) LoadFastestIPsNotBlackOrWhitelisted() error {
	fmt.Println("\n[+] Loading fastest IPs not black- or whitelisted...")

	minRate := 400
	limit := 1000
	sql, err := query.GetFastestIdentities(
		r.base.Scope,
		query.IdentityColumnsIP,
		minRate,
		"WHERE terminating_rule IN (VALUES 'Default_Action', 'rate-limit')", // means requests went through the WAF without any explicit action, except possible rate-limit blocks
		limit,
	)
	if err != nil {
		return fmt.Errorf("rendering sql: %s", err)
	}

	if err := r.base.RunQuery(sql, "fastest-ips-not-black-or-whitelisted"); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	return nil
}

func (r *RateLimitReportLoader) LoadFastestBotUserAgentsNotBlackOrWhitelisted() error {
	fmt.Println("\n[+] Loading fastest Bot User-Agents not black- or whitelisted...")

	minRate := 50 // only bot traffic that is not occasional and slow
	limit := 1000
	sql, err := query.GetFastestIdentities(
		r.base.Scope,
		query.IdentityColumnsUserAgent,
		minRate,
		fmt.Sprintf(
			"WHERE terminating_rule IN (VALUES 'Default_Action', 'rate-limit') AND signal_nobrowser AND user_agent NOT IN (VALUES %s)",
			boringUserAgentValues(),
		), // means requests went through the WAF without any explicit action, except possible rate-limit blocks, and that user agent suggests the client is not a browser
		limit,
	)
	if err != nil {
		return fmt.Errorf("rendering sql: %s", err)
	}

	if err := r.base.RunQuery(sql, "fastest-bot-user-agents-not-black-or-whitelisted"); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	return nil
}

func boringUserAgentValues() string {
	boringUserAgents := []string{
		"ios-de-1.0.0",
	}

	return "'" + strings.Join(boringUserAgents, "','") + "'"
}
