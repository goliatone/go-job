package sqlutil

import (
	"fmt"
	"strings"
)

// ValidateIdentifier ensures an identifier is non-empty and composed of safe SQL identifier parts.
// Supports schema-qualified names in the form `schema.table`.
func ValidateIdentifier(label, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s identifier required", label)
	}
	parts := strings.Split(value, ".")
	for _, part := range parts {
		if !isIdentifierPart(part) {
			return fmt.Errorf("invalid %s identifier %q", label, value)
		}
	}
	return nil
}

func isIdentifierPart(part string) bool {
	if part == "" {
		return false
	}
	for idx := 0; idx < len(part); idx++ {
		ch := part[idx]
		isLetter := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
		isDigit := ch >= '0' && ch <= '9'
		if idx == 0 {
			if !isLetter && ch != '_' {
				return false
			}
			continue
		}
		if !isLetter && !isDigit && ch != '_' {
			return false
		}
	}
	return true
}
