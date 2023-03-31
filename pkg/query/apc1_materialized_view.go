package query

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
)

//go:embed statements/apc1_materialized_view.sql
var APC1MaterializedViewQuery string

func CreateAPC1MaterializedView(scope Scope) (string, error) {
	tpl, err := template.New("query").Parse(APC1MaterializedViewQuery)
	if err != nil {
		return "", err
	}

	data := struct {
		WAF      string
		WafTable string
		Year     string
		Month    string
		Day      string
	}{
		WAF:      fmt.Sprintf("%s", scope.Waf),
		WafTable: getTable(scope.Waf),
		Year:     fmt.Sprintf("%d", scope.Year),
		Month:    fmt.Sprintf("%02d", scope.Month),
		Day:      fmt.Sprintf("%02d", scope.Day),
	}

	var out bytes.Buffer
	if err := tpl.Execute(&out, data); err != nil {
		return "", err
	}

	return out.String(), nil
}
