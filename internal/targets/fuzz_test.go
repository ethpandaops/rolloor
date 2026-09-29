package targets

import (
	"math"
	"testing"
)

func FuzzParseSelector(f *testing.F) {
	for _, s := range []string{"client=a", "id=a-1/cl,role=cl", " node = n1 ", "a=b=c", "a=b,a=c", "=x", "k="} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		sel, err := ParseSelector(s)
		if err != nil {
			return
		}

		again, err := ParseSelector(sel.String())
		if err != nil || again.String() != sel.String() {
			t.Fatalf("%q renders as %q, which parses to %q (%v)", s, sel.String(), again.String(), err)
		}
	})
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{
		good,
		"- {id: a, node: n, weight: 1, image: org/a:t, labels: {client: a, owner: a}}\n",
		"- {id: a, node: n, weight: .nan, image: org/a:t, labels: {client: a, owner: a}}\n",
		"- {id: a, node: n, weight: .inf, image: org/a:t, labels: {client: a, owner: a}}\n",
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		set, err := Parse(raw, &testRules)
		if err != nil {
			return
		}

		// The budget is a share of the total weight; it must be a real,
		// non-negative number that the nodes add up to.
		var sum float64

		for _, n := range set.Nodes() {
			w := set.NodeWeight(n)
			if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 {
				t.Fatalf("node %s weighs %v", n, w)
			}

			sum += w
		}

		if sum != set.TotalWeight() {
			t.Fatalf("nodes weigh %v in all, total says %v", sum, set.TotalWeight())
		}

		for i := range set.Targets {
			if got, ok := set.Get(set.Targets[i].ID); !ok || got.ID != set.Targets[i].ID {
				t.Fatalf("target %q is not found by its id", set.Targets[i].ID)
			}
		}
	})
}
