package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	ownHTTP  = "http://rolloor.example"
	ownHTTPS = "https://rolloor.example"
)

func TestSameOrigin(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target string
		header map[string]string
		want   bool
	}{
		{"fetch metadata", ownHTTP, map[string]string{headerFetchSite: fetchSameOrigin}, true},
		{"typed into the bar", ownHTTP, map[string]string{headerFetchSite: fetchUserInitiated}, true},
		{"origin", ownHTTP, map[string]string{headerOrigin: ownHTTP}, true},
		{"fetch metadata and origin agree", ownHTTPS, map[string]string{headerFetchSite: fetchSameOrigin, headerOrigin: ownHTTPS}, true},
		{"opaque origin beside fetch metadata", ownHTTP, map[string]string{headerFetchSite: fetchSameOrigin, headerOrigin: originOpaque}, false},
		{"referer", ownHTTP, map[string]string{headerReferer: ownHTTP + "/groups/client/a"}, true},
		{"https origin over an unmarked plain HTTP request", ownHTTP, map[string]string{headerOrigin: ownHTTPS}, false},
		{"https origin over a proxy that says so", ownHTTP, map[string]string{headerOrigin: ownHTTPS, headerForwardedProto: schemeHTTPS}, true},

		{"no signal", ownHTTP, nil, false},
		{"privacy-stripped origin alone", ownHTTP, map[string]string{headerOrigin: originOpaque}, false},
		{"cross-site", ownHTTP, map[string]string{headerFetchSite: fetchCrossSite}, false},
		{"a sibling on the same site", ownHTTP, map[string]string{headerFetchSite: "same-site"}, false},
		{"an unknown fetch site", ownHTTP, map[string]string{headerFetchSite: "elsewhere"}, false},
		{"another site's origin", ownHTTP, map[string]string{headerOrigin: foreignOrigin}, false},
		{"a sibling subdomain's origin", ownHTTP, map[string]string{headerOrigin: "http://app.rolloor.example"}, false},
		{"a sibling port's origin", ownHTTP, map[string]string{headerOrigin: ownHTTP + ":8443"}, false},
		{"plain http against TLS", ownHTTPS, map[string]string{headerOrigin: ownHTTP}, false},
		{"plain http against forwarded TLS", ownHTTP, map[string]string{headerOrigin: ownHTTP, headerForwardedProto: schemeHTTPS}, false},
		{"plain http referer against TLS", ownHTTPS, map[string]string{headerReferer: ownHTTP + "/"}, false},
		{"another scheme", ownHTTP, map[string]string{headerOrigin: "ftp://rolloor.example"}, false},
		{"fetch metadata contradicted by origin", ownHTTP, map[string]string{headerFetchSite: fetchSameOrigin, headerOrigin: foreignOrigin}, false},
		{"fetch metadata contradicted by referer", ownHTTP, map[string]string{headerFetchSite: fetchSameOrigin, headerReferer: foreignOrigin + "/"}, false},
		{"origin contradicted by referer", ownHTTP, map[string]string{headerOrigin: ownHTTP, headerReferer: foreignOrigin + "/"}, false},
		{"an origin that is not a URL", ownHTTP, map[string]string{headerOrigin: "http://[::1"}, false},
		{"a referer without a host", ownHTTP, map[string]string{headerReferer: "/groups/client/a"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, tc.target+"/api/v1/actions/sync", http.NoBody)

			for k, v := range tc.header {
				r.Header.Set(k, v)
			}

			require.Equal(t, tc.want, SameOrigin(r))
		})
	}
}
