package query

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
)

//go:embed statements/requests_blocked_by.sql
var RequestsBlockedByQuery string

func GetRequestsBlockedBy(scope Scope, identityCols IdentityColumns, terminatingRules TerminatingRules, limit int) (string, error) {
	tpl, err := template.New("query").Parse(RequestsBlockedByQuery)
	if err != nil {
		return "", err
	}

	data := struct {
		WafTable         string
		Year             string
		Month            string
		Day              string
		IdentityCols     IdentityColumns
		TerminatingRules string
		Limit            string
	}{
		WafTable:         getTable(scope.Waf),
		Year:             fmt.Sprintf("%d", scope.Year),
		Month:            fmt.Sprintf("%02d", scope.Month),
		Day:              fmt.Sprintf("%02d", scope.Day),
		IdentityCols:     identityCols,
		TerminatingRules: terminatingRules.Values(),
		Limit:            fmt.Sprintf("%d", limit),
	}

	var out bytes.Buffer
	if err := tpl.Execute(&out, data); err != nil {
		return "", err
	}

	return out.String(), nil
}
