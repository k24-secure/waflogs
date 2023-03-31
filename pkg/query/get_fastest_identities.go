package query

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
)

//go:embed statements/get_fastest_identities.sql
var GetFastestIdentitiesQuery string

func GetFastestIdentities(scope Scope, identityCols IdentityColumns, minRate int, customWhereClause string, limit int) (string, error) {
	tpl, err := template.New("query").Parse(GetFastestIdentitiesQuery)
	if err != nil {
		return "", err
	}

	data := struct {
		WafTable          string
		Year              string
		Month             string
		Day               string
		IdentityCols      IdentityColumns
		MinRate           string
		CustomWhereClause string
		Limit             string
	}{
		WafTable:          getTable(scope.Waf),
		Year:              fmt.Sprintf("%d", scope.Year),
		Month:             fmt.Sprintf("%02d", scope.Month),
		Day:               fmt.Sprintf("%02d", scope.Day),
		IdentityCols:      identityCols,
		MinRate:           fmt.Sprintf("%d", minRate),
		CustomWhereClause: customWhereClause,
		Limit:             fmt.Sprintf("%d", limit),
	}

	var out bytes.Buffer
	if err := tpl.Execute(&out, data); err != nil {
		return "", err
	}

	return out.String(), nil
}
