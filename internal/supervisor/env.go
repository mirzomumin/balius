package supervisor

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// loadEnv (re)builds the environment handed to the workers: the
// supervisor's own, plus the variables of the configured env files that
// are not already set (the environment always wins over files).
func (s *Supervisor) loadEnv() error {
	env := append([]string{}, s.baseEnv...)
	if len(s.cfg.EnvFiles) == 0 {
		s.env = env
		return nil
	}
	present := map[string]bool{}
	for _, kv := range s.baseEnv {
		if i := strings.IndexByte(kv, '='); i > 0 {
			present[kv[:i]] = true
		}
	}
	for _, file := range s.cfg.EnvFiles {
		values, err := parseEnvFile(file)
		if err != nil {
			return fmt.Errorf("unable to load env file %s: %w", file, err)
		}
		for _, kv := range values {
			if !present[kv[0]] {
				present[kv[0]] = true
				env = append(env, kv[0]+"="+kv[1])
			}
		}
	}
	s.env = env
	return nil
}

// parseEnvFile reads a dotenv file: `KEY=value` lines, optional `export`
// prefix, `#` comments, single or double quoted values (escapes are
// honoured inside double quotes).
func parseEnvFile(path string) ([][2]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out [][2]string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" || strings.ContainsAny(key, " \t") {
			continue
		}
		out = append(out, [2]string{key, parseEnvValue(strings.TrimSpace(value))})
	}
	return out, scanner.Err()
}

func parseEnvValue(v string) string {
	if len(v) >= 2 {
		switch v[0] {
		case '\'':
			if end := strings.IndexByte(v[1:], '\''); end >= 0 {
				return v[1 : 1+end]
			}
		case '"':
			var b strings.Builder
			for i := 1; i < len(v); i++ {
				c := v[i]
				if c == '"' {
					return b.String()
				}
				if c == '\\' && i+1 < len(v) {
					i++
					switch v[i] {
					case 'n':
						b.WriteByte('\n')
					case 'r':
						b.WriteByte('\r')
					case 't':
						b.WriteByte('\t')
					default:
						b.WriteByte(v[i])
					}
					continue
				}
				b.WriteByte(c)
			}
		}
	}
	// unquoted: an inline comment starts at " #"
	if i := strings.Index(v, " #"); i >= 0 {
		v = strings.TrimSpace(v[:i])
	}
	return v
}
