package config

import (
	"math"
	"testing"
)

func FuzzParseFraction(f *testing.F) {
	for _, s := range []string{"10%", "0", "7", "100%", "0.5%", " 3 ", "1e2%", "NaN%", "Inf%", "-0%"} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		fr, err := ParseFraction(s)
		if err != nil {
			return
		}

		for _, total := range []int{0, 1, 3, 10, 1000} {
			if n := fr.Of(total); n < 0 || n > total {
				t.Fatalf("%q of %d is %d", s, total, n)
			}
		}

		for _, total := range []float64{0, 1, 250.5} {
			w := fr.OfWeight(total)
			if math.IsNaN(w) || w < 0 || (fr.IsPercent && w > total) {
				t.Fatalf("%q of weight %v is %v", s, total, w)
			}
		}

		again, err := ParseFraction(fr.String())
		if err != nil || again.IsPercent != fr.IsPercent || again.Percent != fr.Percent || again.Count != fr.Count {
			t.Fatalf("%q renders as %q, which parses to %+v (%v)", s, fr.String(), again, err)
		}
	})
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{
		"environment: e\n",
		"environment: e\ndisruptionBudget: {maxUnavailable: 25%}\nstrategy: {firstBatch: 1, batchSize: 50%}\n",
		"environment: e\nstrategies:\n  fast: {batchSize: 100%, waves: false, soak: {duration: 0s}}\n",
		"environment: e\ndisruptionBudget: {maxUnavailable: NaN%}\n",
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		cfg, err := Parse(raw)
		if err != nil {
			return
		}

		if w := cfg.DisruptionBudget.MaxUnavailable.OfWeight(100); math.IsNaN(w) || w < 0 {
			t.Fatalf("maxUnavailable %s allows %v of 100", cfg.DisruptionBudget.MaxUnavailable, w)
		}

		strategies := map[string]Strategy{"": cfg.Strategy}
		for name, st := range cfg.Strategies {
			strategies[name] = st
		}

		// Every strategy cuts batches of at least one node, or a rollout
		// would wait forever.
		for name, st := range strategies {
			for _, total := range []int{1, 2, 7, 100} {
				for n := 1; n <= 3; n++ {
					if size := st.BatchFor(n, total); size < 1 || size > total {
						t.Fatalf("strategy %q: batch %d of %d nodes has %d", name, n, total, size)
					}
				}
			}
		}
	})
}
