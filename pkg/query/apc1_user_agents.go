package query

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
)

//go:embed statements/apc1_user_agents.sql
var APC1UserAgentsQuery string

func GetAPC1UserAgents(scope Scope, limit int) (string, error) {
	tpl, err := template.New("query").Parse(APC1UserAgentsQuery)
	if err != nil {
		return "", err
	}

	data := struct {
		WAF   string
		Year  string
		Month string
		Day   string
		Limit string
	}{
		WAF:   fmt.Sprintf("%s", scope.Waf),
		Year:  fmt.Sprintf("%d", scope.Year),
		Month: fmt.Sprintf("%02d", scope.Month),
		Day:   fmt.Sprintf("%02d", scope.Day),
		Limit: fmt.Sprintf("%d", limit),
	}

	var out bytes.Buffer
	if err := tpl.Execute(&out, data); err != nil {
		return "", err
	}

	return out.String(), nil
}
