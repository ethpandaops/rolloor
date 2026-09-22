package api

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/ethpandaops/rolloor/internal/targets"
)

// ActionError is why an action over a selector was refused, with the status
// the API answers and the owners involved, so every entry point (JSON API,
// web forms) says the same thing for the same request.
type ActionError struct {
	Status  int
	Code    string
	Message string
	Owners  []string
}

func (e *ActionError) Error() string { return e.Message }

// Authorize decides whether id may act on what sel matches. A sync moves
// whole groups, so wholeGroups widens the affected set to every target in
// the groups touched. A selection that spans several owners needs confirm.
// It returns the matched targets.
func Authorize(set *targets.Set, id *Identity, sel targets.Selector, confirm, wholeGroups bool) ([]targets.Target, error) {
	matched := set.Select(sel)
	if len(matched) == 0 {
		return nil, &ActionError{Status: http.StatusNotFound, Code: "not_found", Message: "selector " + sel.String() + " matches nothing"}
	}

	affected := matched
	if wholeGroups {
		affected = expandGroups(set, matched)
	}

	owners := OwnersOf(set, affected)

	if allowed, why := MayAct(id, owners); !allowed {
		return nil, &ActionError{Status: http.StatusForbidden, Code: "forbidden", Message: why, Owners: owners}
	}

	if len(owners) > 1 && !confirm {
		return nil, &ActionError{
			Status: http.StatusConflict, Code: "confirm_required", Owners: owners,
			Message: fmt.Sprintf("selector %s spans %d owners (%v); confirm to proceed", sel, len(owners), owners),
		}
	}

	return matched, nil
}

// OwnersOf lists the distinct owner values of some targets, sorted.
func OwnersOf(set *targets.Set, ts []targets.Target) []string {
	seen := map[string]struct{}{}

	for i := range ts {
		seen[set.Owner(&ts[i])] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for o := range seen {
		out = append(out, o)
	}

	sort.Strings(out)

	return out
}

// expandGroups returns every target in the groups the given targets belong to.
func expandGroups(set *targets.Set, matched []targets.Target) []targets.Target {
	seen := map[string]struct{}{}

	var out []targets.Target

	for i := range matched {
		g := set.Group(&matched[i])
		if _, done := seen[g]; done {
			continue
		}

		seen[g] = struct{}{}
		out = append(out, set.InGroup(g)...)
	}

	return out
}
