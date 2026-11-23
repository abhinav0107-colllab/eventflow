// Package migrations embeds the versioned SQL migration files so that the
// binaries are self-contained.
package migrations

import "embed"

// FS contains files named NNNN_description.{up,down}.sql.
//
//go:embed *.sql
var FS embed.FS
