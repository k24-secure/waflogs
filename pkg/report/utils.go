package report

import (
	"os"
	"time"
)

// getDay returns year, month and day in numerical form
func getDay(t time.Time) (int, int, int) {
	year, month, day := t.Date()
	return int(year), int(month), int(day)
}

func ensureDirExists(path string) error {
	return os.MkdirAll(path, 0755)
}
