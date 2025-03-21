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

func NewYAMLMetadataParser() *yamlMetadataParser {
	return &yamlMetadataParser{
		patterns: []MatchPattern{
			{
				Name:          "yaml",
				StartPattern:  `^---\s*$`,
				EndPattern:    `^---\s*$`,
				CommentPrefix: "",
			},
			{
				Name:          "javascript",
				StartPattern:  `^//\s*config`,
				EndPattern:    `^(?!//)`, // negative lookahead for comment prefix
				CommentPrefix: "//",
			},
			{
				Name:          "shell",
				StartPattern:  `^#\s*config`,
				EndPattern:    `^(?!#)`, // stop at first non-comment line
				CommentPrefix: "#",
			},
			{
				Name:          "sql",
				StartPattern:  `^--\s*config`,
				EndPattern:    `^(?!--)`, // stop at first non-comment line
				CommentPrefix: "--",
			},
		},
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
					// Script content is all lines after the end marker.
					if end+1 < len(lines) {
						scriptContent = string(bytes.Join(lines[end+1:], []byte("\n")))
					}
				} else {
					// Comment-based metadata.
					end := len(lines)
					for j := start; j < len(lines); j++ {
						if !bytes.HasPrefix(lines[j], []byte(pattern.CommentPrefix)) {
							end = j
							break
						}
					}
					metadataLines = lines[start:end]
					scriptContent = string(bytes.Join(lines[end:], []byte("\n")))
					// Remove comment prefix from metadata lines.
					for i, line := range metadataLines {
						line = bytes.TrimPrefix(line, []byte(pattern.CommentPrefix))
						if len(line) > 0 && line[0] == ' ' {
							line = line[1:]
						}
						metadataLines[i] = line
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
