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
	IsBlock       bool // true for block comment styles (e.g. /** ... */)
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
		Name:          "javascript", // single line comments like // config
		StartPattern:  `^/{2,}\s*config`,
		EndPattern:    `^(?!/{2,})`,
		CommentPrefix: "//",
		IsBlock:       false,
	},
	{
		Name:          "javascript_block", // block style comments /** config ... */
		StartPattern:  `^/\*\*\s*config(.*)$`,
		EndPattern:    `^\*/`,
		CommentPrefix: "*",
		IsBlock:       true,
	},
	{
		Name:          "shell",
		StartPattern:  `^#{1,}\s*config`,
		EndPattern:    `^(?!#{1,})`,
		CommentPrefix: "#",
		IsBlock:       false,
	},
	{
		Name:          "sql",
		StartPattern:  `^-{2,}\s*config`,
		EndPattern:    `^(?!-{2,})`,
		CommentPrefix: "--",
		IsBlock:       false,
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
	// Split the file into lines.
	lines := bytes.Split(content, []byte("\n"))

	for i, origLine := range lines {
		line := bytes.TrimSpace(origLine)
		for _, pattern := range p.patterns {
			re := regexp.MustCompile(pattern.StartPattern)
			if re.Match(line) {
				if pattern.IsBlock {
					var metadataLines [][]byte
					// capture any text after "config" on the first line
					submatches := re.FindSubmatch(line)
					if len(submatches) > 1 && len(submatches[1]) > 0 {
						metadataLines = append(metadataLines, bytes.TrimSpace(submatches[1]))
					}

					endRegex := regexp.MustCompile(pattern.EndPattern)
					j := i + 1
					for ; j < len(lines); j++ {
						trimmed := bytes.TrimSpace(lines[j])
						if endRegex.Match(trimmed) {
							break
						}
						// remove the comment prefix from the trimmed line
						metadataLines = append(metadataLines, stripCommentPrefix(trimmed, pattern.CommentPrefix))
					}

					scriptContent := ""
					if j+1 < len(lines) {
						// preserve the original script lines (with their spacing)
						scriptContent = string(bytes.Join(lines[j+1:], []byte("\n")))
					}

					metadataContent := bytes.Join(metadataLines, []byte("\n"))
					cfg, err := parseRawConfig(metadataContent)
					return cfg, scriptContent, err
				}

				// YAML style with no comment prefix
				if pattern.CommentPrefix == "" {
					endRegex := regexp.MustCompile(pattern.EndPattern)
					end := len(lines)
					for j := i + 1; j < len(lines); j++ {
						trimmed := bytes.TrimSpace(lines[j])
						if endRegex.Match(trimmed) {
							end = j
							break
						}
					}

					// trimmed metadata lines
					var metadataLines [][]byte
					for j := i + 1; j < end; j++ {
						metadataLines = append(metadataLines, bytes.TrimSpace(lines[j]))
					}

					scriptContent := ""
					if end+1 < len(lines) {
						scriptContent = string(bytes.Join(lines[end+1:], []byte("\n")))
					}

					metadataContent := bytes.Join(metadataLines, []byte("\n"))
					cfg, err := parseRawConfig(metadataContent)
					return cfg, scriptContent, err
				}

				// single line comment branch
				commentRegex := commentRegexFor(pattern.CommentPrefix)
				end := len(lines)
				for j := i + 1; j < len(lines); j++ {
					trimmed := bytes.TrimSpace(lines[j])
					if !commentRegex.Match(trimmed) {
						end = j
						break
					}
				}

				var metadataLines [][]byte
				for j := i + 1; j < end; j++ {
					// use the trimmed version of the line
					metadataLines = append(metadataLines, stripCommentPrefix(bytes.TrimSpace(lines[j]), pattern.CommentPrefix))
				}
				scriptContent := string(bytes.Join(lines[end:], []byte("\n")))
				metadataContent := bytes.Join(metadataLines, []byte("\n"))
				cfg, err := parseRawConfig(metadataContent)
				return cfg, scriptContent, err
			}
		}
	}

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

// commentRegexFor returns a regex that will match a comment prefix
// repeated at least as many times as in the configured prefix
func commentRegexFor(prefix string) *regexp.Regexp {
	allSame := true
	for _, c := range prefix {
		if c != rune(prefix[0]) {
			allSame = false
			break
		}
	}

	if allSame {
		minCount := len(prefix)
		// prefix "//" -> regex becomes ^/{2,}
		return regexp.MustCompile("^" + regexp.QuoteMeta(strings.Repeat(string(prefix[0]), minCount)) + "+")
	}
	// require exactly the configured prefix
	return regexp.MustCompile("^" + regexp.QuoteMeta(prefix))
}

// stripCommentPrefix removes the repeated comment marker (and an optional space) from the beginning of the trimmed line.
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
		re = regexp.MustCompile("^" + regexp.QuoteMeta(strings.Repeat(string(prefix[0]), minCount)) + `+\s?`)
	} else {
		re = regexp.MustCompile("^" + regexp.QuoteMeta(prefix) + `\s?`)
	}
	return re.ReplaceAll(line, []byte(""))
}
