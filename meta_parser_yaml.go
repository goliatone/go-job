package job

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

type yamlMetadataParser struct{}

func NewYAMLMetadataParser() *yamlMetadataParser {
	return &yamlMetadataParser{}
}

func (p *yamlMetadataParser) Parse(content []byte) (Config, string, error) {
	patterns := []struct {
		startPattern  string
		endPattern    string
		commentPrefix string
	}{
		{`^---\s*$`, `^---\s*$`, ""},                 // YAML style
		{`^//\s+handler\s+options`, `^//\s*$`, "//"}, // JS style
		{`^--\s+handler\s+options`, `^--\s*$`, "--"}, // SQL style
		{`^#\s+handler\s+options`, `^#\s*$`, "#"},    // Shell style
	}

	emptyConfig := Config{
		Schedule: "* * * * *", // Default to every minute
		Timeout:  time.Minute, // Default to 1 minute timeout
	}

	for _, pattern := range patterns {
		startRegex := regexp.MustCompile(pattern.startPattern)
		endRegex := regexp.MustCompile(pattern.endPattern)

		startIndex := -1
		endIndex := -1

		lines := bytes.Split(content, []byte("\n"))
		for i, line := range lines {
			if startIndex == -1 && startRegex.Match(line) {
				startIndex = i
				continue
			}

			if startIndex != -1 && endRegex.Match(line) && i > startIndex {
				endIndex = i
				break
			}
		}
		if startIndex != -1 && endIndex != -1 {
			metadataLines := lines[startIndex+1 : endIndex]
			// remove comment prefixes if any
			if pattern.commentPrefix != "" {
				for i, line := range metadataLines {
					trimmed := bytes.TrimPrefix(line, []byte(pattern.commentPrefix))
					// make sure there is at least one space after the comment prefix
					if len(trimmed) > 0 && trimmed[0] == ' ' {
						trimmed = trimmed[1:]
					}
					metadataLines[i] = trimmed
				}
			}

			metadataContent := bytes.Join(metadataLines, []byte("\n"))

			scriptContent := string(bytes.Join(lines[endIndex+1:], []byte("\n")))

			// 1) try to unmarshal directly to JobConfig
			var config Config
			if err := yaml.Unmarshal(metadataContent, &config); err != nil {
				//1.1) if that fails try to build a map
				var rawConfig map[string]any
				if err := yaml.Unmarshal(metadataContent, &rawConfig); err != nil {
					return emptyConfig, scriptContent, fmt.Errorf("failed to parse metadata: %w", err)
				}

				config, err = processRawConfig(rawConfig)
				if err != nil {
					return emptyConfig, scriptContent, err
				}
			}

			return config, scriptContent, nil
		}
	}
	return emptyConfig, string(content), nil
}

func processRawConfig(raw map[string]any) (Config, error) {
	config := Config{
		Schedule: "* * * * *", // every minute
		Timeout:  time.Minute,
		Metadata: make(map[string]any),
	}

	for k, v := range raw {
		switch k {
		case "schedule":
			if str, ok := v.(string); ok {
				config.Schedule = str
			}
		case "timeout":
			switch tv := v.(type) {
			case string:
				d, err := time.ParseDuration(tv)
				if err != nil {
					return config, fmt.Errorf("invalid duration format for timeout: %s", tv)
				}
				config.Timeout = d
			case int:
				config.Timeout = time.Duration(tv) * time.Second
			case float64:
				config.Timeout = time.Duration(tv) * time.Second
			}
		case "retries", "max_retries":
			retries, err := toInt(v)
			if err != nil {
				return config, fmt.Errorf("%s must be a numeric value: %w", k, err)
			}
			config.Retries = retries
		case "debug":
			if b, ok := v.(bool); ok {
				config.Debug = b
			}
		case "run_once":
			if b, ok := v.(bool); ok {
				config.RunOnce = b
			}
		case "script_type":
			if str, ok := v.(string); ok {
				config.ScriptType = str
			}
		case "transaction":
			if b, ok := v.(bool); ok {
				config.Transaction = b
			}
		case "env":
			if envMap, ok := v.(map[any]any); ok {
				config.Env = make(map[string]string)
				for ek, ev := range envMap {
					if ekStr, ok := ek.(string); ok {
						config.Env[ekStr] = fmt.Sprintf("%v", ev)
					}
				}
			} else if envMap, ok := v.(map[string]any); ok {
				config.Env = make(map[string]string)
				for ek, ev := range envMap {
					config.Env[ek] = fmt.Sprintf("%v", ev)
				}
			} else if envMap, ok := v.(map[string]string); ok {
				config.Env = envMap
			}
		default:
			config.Metadata[k] = v
		}
	}
	return config, nil
}

func toInt(v any) (int, error) {
	switch tv := v.(type) {
	case int:
		return tv, nil
	case float64:
		return int(tv), nil
	case string:
		return strconv.Atoi(strings.TrimSpace(tv))
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}
