package supervisor

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// processRSS returns the resident set size of a process, in bytes.
func processRSS(pid int) (int64, error) {
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
		if err != nil {
			return 0, err
		}
		fields := strings.Fields(string(data))
		if len(fields) < 2 {
			return 0, fmt.Errorf("unexpected statm format")
		}
		pages, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return pages * int64(os.Getpagesize()), nil
	}
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, err
	}
	return kb * 1024, nil
}
