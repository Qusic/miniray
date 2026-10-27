package netutil

import (
	"strings"
	"testing"
)

func TestLabel(t *testing.T) {
	for text, valid := range map[string]bool{
		"a":                     true,
		"0":                     true,
		"node-1":                true,
		strings.Repeat("a", 63): true,
		"":                      false,
		strings.Repeat("a", 64): false,
		"-node":                 false,
		"node-":                 false,
		"Node":                  false,
		"node_1":                false,
		"node.1":                false,
		"nöde":                  false,
	} {
		var label Label
		err := label.UnmarshalText([]byte(text))
		if (err == nil) != valid || (valid && string(label) != text) {
			t.Errorf("Label %q = %q, %v; want valid = %t", text, label, err, valid)
		}
	}
}
