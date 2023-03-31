package printer

import (
	"encoding/csv"
	"fmt"
	"io"
	"kfzteile24/waflogs/pkg/query"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/guptarohit/asciigraph"
)

const DataDir = "./data"

type RateLimitReportPrinter struct {
	Waf query.WAF
}

func NewRateLimitReportPrinter(waf query.WAF) *RateLimitReportPrinter {
	return &RateLimitReportPrinter{
		Waf: waf,
	}
}

func (rp *RateLimitReportPrinter) Print() error {
	days, err := rp.listDays()
	if err != nil {
		return fmt.Errorf("listing days: %s", err)
	}

	var data []float64
	var max int
	for _, day := range days {
		n, err := rp.getFastestBotUserAgentOf(day)
		if err != nil {
			return fmt.Errorf("getting fastest bot user agent of day %s: %s", day, err)
		}
		data = append(data, float64(n))
		if n > max {
			max = n
		}
		fmt.Printf("%s: %d\n", day, n)
	}

	for i := range data {
		data[i] = data[i] / float64(max) * 20.0
	}

	graph := asciigraph.Plot(data)

	fmt.Println(graph)

	return nil
}

func (rp *RateLimitReportPrinter) listDays() ([]string, error) {
	files, err := os.ReadDir(rp.getReportDir())
	if err != nil {
		return nil, fmt.Errorf("reading dir: %s", err)
	}

	var days []string
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		days = append(days, file.Name())
	}

	sort.Strings(days) // just to be sure, should be sorted anyways

	return days, nil
}

func (rp *RateLimitReportPrinter) getReportDir() string {
	return filepath.Join(DataDir, rp.Waf.String(), "rate-limit-report")
}

func (rp *RateLimitReportPrinter) getFastestBotUserAgentOf(day string) (int, error) {
	path := filepath.Join(rp.getReportDir(), day, "fastest-bot-user-agents-not-black-or-whitelisted.csv")

	records, err := readCsv(path, 2)
	if err != nil {
		return 0, fmt.Errorf("reading csv at %s: %s", path, err)
	}

	if len(records) < 2 {
		return 0, nil
	}
	if len(records[1]) < 5 {
		return 0, fmt.Errorf("unexpected csv forat of %s: %s", path, err)
	}

	out, err := strconv.Atoi(records[1][4])
	if err != nil {
		return 0, fmt.Errorf("parsing number at records[1][4]: %s", err)
	}

	return out, nil
}

func readCsv(path string, max int) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("openeing file: %s", err)
	}
	defer f.Close()

	r := csv.NewReader(f)

	var out [][]string
	for {
		if len(out) == max {
			break
		}

		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading record: %s", err)
		}

		out = append(out, record)
	}

	return out, nil
}
