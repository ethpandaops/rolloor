package registry

import "testing"

func FuzzParseReference(f *testing.F) {
	for _, s := range []string{"nginx", "org/app:v1", "localhost:5000/app:latest", "ghcr.io/org/app:tag", "a:b:c", "host:1/x", "@sha256:x"} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		ref, err := ParseReference(s)
		if err != nil {
			return
		}

		again, err := ParseReference(ref.String())
		if err != nil || again != ref {
			t.Fatalf("%q renders as %q, which parses to %+v (%v)", s, ref.String(), again, err)
		}
	})
}
