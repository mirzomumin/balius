// Package logx is the runtime logger used by the Go side of tulpar.
//
// Output mirrors the format of the Python-side `_tulpar` logger
// (`[LEVEL] message` on stdout), so supervisor, Go worker runtime and Python
// produce the same kind of lines.
package logx

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
)

// Level is a logging severity, numerically aligned with Python's logging.
type Level int32

const (
	LevelNotset   Level = 0
	LevelDebug    Level = 10
	LevelInfo     Level = 20
	LevelWarning  Level = 30
	LevelError    Level = 40
	LevelCritical Level = 50
	levelOff      Level = 100
)

var level atomic.Int32

func init() { level.Store(int32(LevelInfo)) }

var levels = map[string]Level{
	"critical": LevelCritical, "error": LevelError, "warning": LevelWarning, "warn": LevelWarning,
	"info": LevelInfo, "debug": LevelDebug, "notset": LevelNotset,
}

// Configure sets the minimum level; when enabled is false nothing is logged.
func Configure(name string, enabled bool) {
	lvl, ok := levels[strings.ToLower(name)]
	if !ok {
		lvl = LevelInfo
	}
	if !enabled {
		lvl = levelOff
	}
	level.Store(int32(lvl))
}

func emit(lvl Level, name, format string, a []any) {
	if int32(lvl) < level.Load() {
		return
	}
	// a single write per line: *os.File serializes concurrent writes
	_, _ = os.Stdout.WriteString("[" + name + "] " + fmt.Sprintf(format, a...) + "\n")
}

// Debugf logs at debug level.
func Debugf(format string, a ...any) { emit(LevelDebug, "DEBUG", format, a) }

// Infof logs at info level.
func Infof(format string, a ...any) { emit(LevelInfo, "INFO", format, a) }

// Warnf logs at warning level.
func Warnf(format string, a ...any) { emit(LevelWarning, "WARNING", format, a) }

// Errorf logs at error level.
func Errorf(format string, a ...any) { emit(LevelError, "ERROR", format, a) }
