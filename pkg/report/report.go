package report

import (
	"bufio"
	"fmt"
	"kfzteile24/waflogs/pkg/aws"
	"kfzteile24/waflogs/pkg/query"
	"log"
	"os"
	"path/filepath"
)

const DataDir = "./data"

type ReportLoader struct {
	Athena *aws.AthenaClient

	Name  string
	Scope query.Scope
}

func NewReportLoader(a *aws.AthenaClient, waf query.WAF, name string, year int, month int, day int) *ReportLoader {
	return &ReportLoader{
		Athena: a,
		Name:   name,
		Scope: query.Scope{
			Waf:   waf,
			Year:  year,
			Month: month,
			Day:   day,
		},
	}
}

func (r *ReportLoader) getOutDir() string {
	return filepath.Join(DataDir, r.Scope.Waf.String(), r.Name, fmt.Sprintf("%04d-%02d-%02d", r.Scope.Year, r.Scope.Month, r.Scope.Day))
}

func (r *ReportLoader) ensureOutDirExists() error {
	return ensureDirExists(r.getOutDir())
}

func (r *ReportLoader) RunQuery(sql string, name string) error {
	queryPath := filepath.Join(r.getOutDir(), fmt.Sprintf("%s.sql", name))
	if err := os.WriteFile(queryPath, []byte(sql), 0644); err != nil {
		return fmt.Errorf("writing query to disk: %s", err)
	}

	resultsPath := filepath.Join(r.getOutDir(), fmt.Sprintf("%s.csv", name))
	if err := r.Athena.Query(sql, resultsPath); err != nil {
		return fmt.Errorf("running query: %s", err)
	}

	numLines, err := countLines(resultsPath)
	if err != nil {
		return fmt.Errorf("counting lines of result : %s", err)
	}
	if numLines > 0 {
		log.Printf("Query done: number of lines returned: %d\n", numLines-1) // subtract header
	}

	return nil
}

func countLines(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return count, nil
}
