// Package registry resolves image tags to digests and reads the revision label.
package registry

import (
	"fmt"
	"strings"
)

const (
	dockerHubHost    = "docker.io"
	dockerHubAPIHost = "registry-1.docker.io"
	localhost        = "localhost"
	defaultTag       = "latest"
)

// Reference is a parsed repository:tag.
type Reference struct {
	// Host is the registry as written, docker.io when omitted.
	Host string
	// Path is the repository path, with library/ added for Docker Hub official images.
	Path string
	Tag  string
}

// ParseReference splits [host/]path[:tag]. Digests are refused.
func ParseReference(ref string) (Reference, error) {
	if ref == "" {
		return Reference{}, fmt.Errorf("registry: empty reference")
	}

	if strings.Contains(ref, "@") {
		return Reference{}, fmt.Errorf("registry: %q: digests are not accepted here", ref)
	}

	host := dockerHubHost
	rest := ref

	if i := strings.Index(ref, "/"); i > 0 {
		first := ref[:i]
		if strings.ContainsAny(first, ".:") || first == localhost {
			host = first
			rest = ref[i+1:]
		}
	}

	path, tag := rest, defaultTag

	if i := strings.LastIndex(rest, ":"); i >= 0 && !strings.Contains(rest[i:], "/") {
		path, tag = rest[:i], rest[i+1:]
	}

	if path == "" || tag == "" {
		return Reference{}, fmt.Errorf("registry: %q: want repository:tag", ref)
	}

	if host == dockerHubHost && !strings.Contains(path, "/") {
		path = "library/" + path
	}

	return Reference{Host: host, Path: path, Tag: tag}, nil
}

// APIHost is the host to send requests to.
func (r Reference) APIHost() string {
	if r.Host == dockerHubHost {
		return dockerHubAPIHost
	}

	return r.Host
}

// String renders host/path:tag.
func (r Reference) String() string {
	return r.Host + "/" + r.Path + ":" + r.Tag
}
