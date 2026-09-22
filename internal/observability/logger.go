// Package observability holds the contextual logger contract shared by every
// component and the process-wide logger constructor.
package observability

import (
	"context"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

// ContextualLogger is satisfied by both *logrus.Logger and *logrus.Entry, so
// components can accept either and derive their own fields.
type ContextualLogger interface {
	logrus.FieldLogger
	WithContext(ctx context.Context) *logrus.Entry
}

// NewLogger builds the root logger. format is "json" or "text".
func NewLogger(level, format string) (*logrus.Logger, error) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("parse log level %q: %w", level, err)
	}

	log := logrus.New()
	log.SetOutput(os.Stdout)
	log.SetLevel(lvl)

	switch format {
	case "json", "":
		log.SetFormatter(&logrus.JSONFormatter{})
	case "text":
		log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	default:
		return nil, fmt.Errorf("unknown log format %q", format)
	}

	return log, nil
}
