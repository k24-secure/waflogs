package query

import (
	"fmt"
	"strings"
)

// ###########################

type IdentityColumns string

const (
	IdentityColumnsIP        IdentityColumns = "client_ip, country"
	IdentityColumnsUserAgent IdentityColumns = "bot_name, bot_category, user_agent"
)

// ###########################

type WAF int

const (
	WafBC WAF = iota
	WafECP
)

func (w WAF) String() string {
	switch w {
	case WafBC:
		return "BC"
	case WafECP:
		return "ECP"
	default:
		panic(fmt.Sprintf("unreachable code reached: waf %d unknown", w))
	}
}

func getTable(waf WAF) string {
	switch waf {
	case WafBC:
		return "\"waflogs\".\"waf_logs_p\""
	case WafECP:
		return "\"waflogs\".\"waf_logs_ecp_p\""
	default:
		panic(fmt.Sprintf("unreachable code reached: waf %d unknown", waf))
	}
}

// ###########################

type TerminatingRule int

const (
	TerminatingRuleRateLimit TerminatingRule = iota
)

// String returns the ID of the rule
func (t TerminatingRule) String() string {
	switch t {
	case TerminatingRuleRateLimit:
		return "rate-limit"
	default:
		panic(fmt.Sprintf("unreachable code reached: terminating rule %d unknown", t))
	}
}

type TerminatingRules []TerminatingRule

// Values returns the column names as list of SQL values
// e.g.: 'col1', 'col2'
func (ts TerminatingRules) Values() string {
	var cols []string
	for _, t := range ts {
		cols = append(cols, t.String())
	}

	return "'" + strings.Join(cols, "','") + "'"
}
