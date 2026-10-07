package reconcile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
)

func TestFailedReloadRetainsEffectivePausesUntilValidConfig(t *testing.T) {
	const futureGroup = "future"

	h := newHarness(t, testConfig, retryFleet+b1Line)
	h.prime()
	require.NoError(t, h.c.ConfigureGroups(h.ctx, false, map[string]config.Group{
		"a": {Paused: true}, futureGroup: {Paused: true},
	}))
	h.c.ReportConfigError(errFake)
	top, failed, groups := h.c.AutomationStatus()
	require.False(t, top)
	require.True(t, failed)
	require.Equal(t, map[string]bool{"a": true, "b": false, futureGroup: true}, groups)
	require.Error(t, h.c.ConfigureGroups(h.ctx, true, map[string]config.Group{"a": {Strategy: "missing"}}))
	top, failed, groups = h.c.AutomationStatus()
	require.False(t, top)
	require.True(t, failed)
	require.Equal(t, map[string]bool{"a": true, "b": false, futureGroup: true}, groups)
	require.NoError(t, h.c.ConfigureGroups(h.ctx, true, nil))
	h.c.ReportConfigError(nil)
	top, failed, groups = h.c.AutomationStatus()
	require.True(t, top)
	require.False(t, failed)
	require.Equal(t, map[string]bool{"a": true, "b": true}, groups)
	require.NoError(t, h.c.ConfigureGroups(h.ctx, false, nil))
	top, failed, groups = h.c.AutomationStatus()
	require.False(t, top)
	require.False(t, failed)
	require.Equal(t, map[string]bool{"a": false, "b": false}, groups)
}
