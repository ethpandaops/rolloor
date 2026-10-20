package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/reconcile"
)

func historyResponse(t *testing.T, f *fixture) (string, []reconcile.Event) {
	t.Helper()

	req, err := http.NewRequestWithContext(f.ctx, http.MethodGet, f.srv.URL+"/api/v1/history?after=0", http.NoBody)
	require.NoError(t, err)
	res, err := f.srv.Client().Do(req)
	require.NoError(t, err)

	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)

	var events []reconcile.Event
	require.NoError(t, json.NewDecoder(res.Body).Decode(&events))

	return res.Header.Get("Rolloor-History"), events
}

func TestHistoryHeaderIdentifiesReplacedSequence(t *testing.T) {
	f := newFixture(t)
	f.release()
	identity, first := historyResponse(t, f)
	require.NotEmpty(t, identity)
	require.Equal(t, int64(1), first[0].ID)
	again, same := historyResponse(t, f)
	require.Equal(t, identity, again)
	require.Equal(t, first, same)

	other := newFixture(t)
	for range len(first) + 1 {
		other.c.Refresh(other.ctx, admin)
	}

	replaced, events := historyResponse(t, other)
	require.NotEqual(t, identity, replaced)
	require.Greater(t, events[len(events)-1].ID, first[len(first)-1].ID)
	require.Equal(t, int64(1), events[0].ID)
}
