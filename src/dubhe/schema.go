// Package tracingschema shares the MySQL schema with the Python reference.
package tracingschema

import (
	"context"
	"database/sql"
	_ "embed"
	"strings"
)

//go:embed schema.sql
var definition string

func Initialize(ctx context.Context, db *sql.DB) error {
	for statement := range strings.SplitSeq(definition, ";") {
		if strings.TrimSpace(statement) != "" {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	return nil
}
