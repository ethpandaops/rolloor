package ethpandaops

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// TeamsRequest says which coredevs teams feed which owner values.
type TeamsRequest struct {
	// API is the coredevs registry base URL.
	API string
	// Teams maps owner value to the coredevs team slugs whose members it
	// grants.
	Teams map[string][]string
	// Members adds handles to owner values directly.
	Members map[string][]string
	// Out is the teams file to write.
	Out string
}

// RefreshTeams writes the teams file from the registry. Any team that cannot
// be read leaves the existing file untouched.
func RefreshTeams(ctx context.Context, client *http.Client, req *TeamsRequest) (int, error) {
	teams := map[string][]string{}

	for owner, slugs := range req.Teams {
		for _, slug := range slugs {
			u := strings.TrimRight(req.API, "/") + "/api/v1/users/" + url.PathEscape(slug) + "?format=txt"

			handles, err := fetchLines(ctx, client, u)
			if err != nil {
				return 0, fmt.Errorf("team %s: %w", slug, err)
			}

			teams[owner] = append(teams[owner], handles...)
		}
	}

	for owner, handles := range req.Members {
		teams[owner] = append(teams[owner], handles...)
	}

	for owner := range teams {
		slices.Sort(teams[owner])
		teams[owner] = slices.Compact(teams[owner])
	}

	// A map of string lists always encodes.
	raw, _ := yaml.Marshal(teams)
	raw = append([]byte("# Written by rolloor-ethpandaops teams from the coredevs registry; edits are overwritten.\n"), raw...)

	tmp := req.Out + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil { //nolint:gosec // the teams file is not secret and rolloor reads it as another user
		return 0, err
	}

	if err := os.Rename(tmp, req.Out); err != nil {
		_ = os.Remove(tmp)

		return 0, err
	}

	return len(teams), nil
}

func fetchLines(ctx context.Context, client *http.Client, u string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", redact(u), resp.Status)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}

	var out []string

	for line := range strings.SplitSeq(string(raw), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}

	return out, nil
}
