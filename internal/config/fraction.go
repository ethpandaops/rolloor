package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Fraction is either a percentage of a whole ("10%") or an absolute count (3).
type Fraction struct {
	Percent float64
	Count   int
	// IsPercent reports which of the two fields is meaningful.
	IsPercent bool
	// set is true once a value was written explicitly, so an explicit zero
	// is not mistaken for an omitted field.
	set bool
}

// ParseFraction accepts "N%" or an integer.
func ParseFraction(s string) (Fraction, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Fraction{}, fmt.Errorf("empty fraction")
	}

	if pct, ok := strings.CutSuffix(s, "%"); ok {
		p, err := strconv.ParseFloat(pct, 64)
		if err != nil || p < 0 || p > 100 {
			return Fraction{}, fmt.Errorf("fraction %q: want 0-100%%", s)
		}

		return Fraction{Percent: p, IsPercent: true}, nil
	}

	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return Fraction{}, fmt.Errorf("fraction %q: want N%% or a non-negative integer", s)
	}

	return Fraction{Count: n}, nil
}

// Of returns the number of items this fraction selects out of total, at least
// one when total is positive and the fraction is not zero.
func (f Fraction) Of(total int) int {
	if total <= 0 {
		return 0
	}

	if !f.IsPercent {
		if f.Count > total {
			return total
		}

		return f.Count
	}

	if f.Percent == 0 {
		return 0
	}

	n := max(int(math.Ceil(float64(total)*f.Percent/100)), 1)

	return min(n, total)
}

// OfWeight returns the weight this fraction allows out of total weight.
func (f Fraction) OfWeight(total float64) float64 {
	if f.IsPercent {
		return total * f.Percent / 100
	}

	return float64(f.Count)
}

// String renders the fraction the way it was written.
func (f Fraction) String() string {
	if f.IsPercent {
		return strconv.FormatFloat(f.Percent, 'f', -1, 64) + "%"
	}

	return strconv.Itoa(f.Count)
}

// UnmarshalYAML accepts a scalar of either form.
func (f *Fraction) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("fraction: want a scalar, got %v", value.Tag)
	}

	parsed, err := ParseFraction(value.Value)
	if err != nil {
		return err
	}

	parsed.set = true
	*f = parsed

	return nil
}

// MarshalYAML renders the fraction as it was written.
func (f Fraction) MarshalYAML() (any, error) {
	return f.String(), nil
}

// MarshalJSON renders the fraction as a string.
func (f Fraction) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(f.String())), nil
}
