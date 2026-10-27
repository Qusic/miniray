package netutil

import "fmt"

// Label is a lowercase DNS label, such as a node name or an alias.
type Label string

// ParseLabel checks that s is a lowercase DNS label.
func ParseLabel(s string) (Label, error) {
	if !isLabel(s) {
		return "", fmt.Errorf("%q is not a lowercase DNS label", s)
	}
	return Label(s), nil
}

func (l *Label) UnmarshalText(text []byte) error {
	label, err := ParseLabel(string(text))
	if err != nil {
		return err
	}
	*l = label
	return nil
}

func isLabel(s string) bool {
	if len(s) == 0 || len(s) > 63 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}
