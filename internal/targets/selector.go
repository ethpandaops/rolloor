package targets

import (
	"fmt"
	"sort"
	"strings"
)

// Selector matches targets by label values. The keys "id" and "node" match
// the target's own fields; every other key is a label.
type Selector map[string]string

// ParseSelector reads "k=v,k=v".
func ParseSelector(s string) (Selector, error) {
	sel := Selector{}

	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("selector: empty")
	}

	for part := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("selector: %q is not key=value", part)
		}

		sel[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}

	return sel, nil
}

// Match reports whether every key of the selector agrees with the target.
func (sel Selector) Match(t *Target) bool {
	for k, v := range sel {
		switch k {
		case "id":
			if t.ID != v {
				return false
			}
		case "node":
			if t.Node != v {
				return false
			}
		default:
			if t.Labels[k] != v {
				return false
			}
		}
	}

	return true
}

// String renders the selector with sorted keys.
func (sel Selector) String() string {
	keys := make([]string, 0, len(sel))
	for k := range sel {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+sel[k])
	}

	return strings.Join(parts, ",")
}
