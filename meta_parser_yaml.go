package job

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

type MatchPattern struct {
	Name          string
	StartPattern  string
	EndPattern    string
	CommentPrefix string
}

type yamlMetadataParser struct {
	patterns []MatchPattern
}

var DefaultMatchPatterns = []MatchPattern{
	{
		Name:          "yaml",
		StartPattern:  `^---\s*$`,
		EndPattern:    `^---\s*$`,
		CommentPrefix: "",
	},
	{
		Name:          "javascript",
		StartPattern:  `^/{2,}\s*config`, // // allow additional slashes before "config"
		EndPattern:    `^(?!/{2,})`,      // end when the line does not start with at least two slashes
		CommentPrefix: "//",
	},
	{
		Name:          "shell",
		StartPattern:  `^#{1,}\s*config`,
		EndPattern:    `^(?!#{1,})`,
		CommentPrefix: "#",
	},
	{
		Name:          "sql",
		StartPattern:  `^-{2,}\s*config`,
		EndPattern:    `^(?!-{2,})`,
		CommentPrefix: "--",
	},
}

func NewYAMLMetadataParser(patterns ...MatchPattern) *yamlMetadataParser {

	patterns = append(patterns, DefaultMatchPatterns...)

	return &yamlMetadataParser{
		patterns: patterns,
	}
}

// Parse extracts metadata and script content from the given content,
// i.e. the result of
//
//	content, _ := os.ReadFile(path)
//
// It returns a Config, the remaining script minus the config content
// and any errors collected during parsing.
func (p *yamlMetadataParser) Parse(content []byte) (Config, string, error) {
	lines := bytes.Split(content, []byte("\n"))

	for _, pattern := range p.patterns {
		startRegex := regexp.MustCompile(pattern.StartPattern)

		for i, line := range lines {
			if startRegex.Match(line) {
				start := i + 1
				var metadataLines [][]byte
				var scriptContent string

				// YAML branch: look for the end marker using EndPattern.
				if pattern.CommentPrefix == "" {
					endRegex := regexp.MustCompile(pattern.EndPattern)
					end := len(lines)
					for j := start; j < len(lines); j++ {
						if endRegex.Match(lines[j]) {
							end = j
							break
						}
					}

					metadataLines = lines[start:end]
					if end+1 < len(lines) {
						scriptContent = string(bytes.Join(lines[end+1:], []byte("\n")))
					}
				} else {
					commentRegex := commentRegexFor(pattern.CommentPrefix)
					end := len(lines)
					for j := start; j < len(lines); j++ {
						if !commentRegex.Match(lines[j]) {
							end = j
							break
						}
					}
					metadataLines = lines[start:end]
					scriptContent = string(bytes.Join(lines[end:], []byte("\n")))
					// Strip the comment prefix from each metadata line.
					for i, line := range metadataLines {
						metadataLines[i] = stripCommentPrefix(line, pattern.CommentPrefix)
					}
				}

				metadataContent := bytes.Join(metadataLines, []byte("\n"))
				cfg, err := parseRawConfig(metadataContent)
				return cfg, scriptContent, err
			}
		}
	}

	// no metadata matched! return default config and full content
	return Config{
		Schedule: DefaultSchedule,
		Timeout:  DefaultTimeout,
	}, string(content), nil
}

type rawConfig struct {
	Schedule    string            `yaml:"schedule"`
	Retries     int               `yaml:"retries"`
	Timeout     string            `yaml:"timeout"`
	NoTimeout   bool              `yaml:"no_timeout"`
	Debug       bool              `yaml:"debug"`
	RunOnce     bool              `yaml:"run_once"`
	Env         map[string]string `yaml:"env"`
	ScriptType  string            `yaml:"script_type"`
	Transaction bool              `yaml:"transaction"`
	Metadata    map[string]any    `yaml:"metadata"`
}

func parseRawConfig(data []byte) (Config, error) {
	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return Config{}, err
	}

	cfg := Config{
		Schedule:    raw.Schedule,
		Retries:     raw.Retries,
		NoTimeout:   raw.NoTimeout,
		Debug:       raw.Debug,
		RunOnce:     raw.RunOnce,
		ScriptType:  raw.ScriptType,
		Transaction: raw.Transaction,
		Metadata:    raw.Metadata,
		Env:         raw.Env,
		Timeout:     DefaultTimeout,
	}

	var errs error

	if raw.Timeout != "" {
		// try first for 300s
		d, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			// assume int, but support 30_000
			cleaned := strings.ReplaceAll(raw.Timeout, "_", "")
			if seconds, err2 := strconv.Atoi(cleaned); err2 == nil {
				d = time.Duration(seconds) * time.Second
			} else {
				errs = errors.Join(errs, errors.New(fmt.Sprintf("invalid timeout duration: %s", raw.Timeout)))
			}
		}
		// success, set it
		if d > 0 {
			cfg.Timeout = d
		}
	}

	if cfg.Schedule == "" {
		cfg.Schedule = DefaultSchedule
	}

	return cfg, errs
}

// commentRegexFor returns a regex that will match a comment prefix repeated at least as many times as in the configured prefix.
// For instance, if prefix is "--", then the regex will match two or more '-' at the start.
func commentRegexFor(prefix string) *regexp.Regexp {
	// Check if all characters are identical.
	allSame := true
	for _, c := range prefix {
		if c != rune(prefix[0]) {
			allSame = false
			break
		}
	}
	if allSame {
		minCount := len(prefix)
		// e.g. for prefix "//" -> regex becomes ^/{2,}
		return regexp.MustCompile("^" + regexp.QuoteMeta(strings.Repeat(string(prefix[0]), minCount)) + "+")
	}
	// Fallback: require exactly the configured prefix.
	return regexp.MustCompile("^" + regexp.QuoteMeta(prefix))
}

// stripCommentPrefix removes the repeated comment marker (and one optional space) from the beginning of the line.
func stripCommentPrefix(line []byte, prefix string) []byte {
	allSame := true
	for _, c := range prefix {
		if c != rune(prefix[0]) {
			allSame = false
			break
		}
	}
	var re *regexp.Regexp
	if allSame {
		minCount := len(prefix)
		// Matches the repeated marker and an optional space.
		re = regexp.MustCompile("^" + regexp.QuoteMeta(strings.Repeat(string(prefix[0]), minCount)) + "+\\s?")
	} else {
		re = regexp.MustCompile("^" + regexp.QuoteMeta(prefix) + "\\s?")
	}
	return re.ReplaceAll(line, []byte(""))
}
