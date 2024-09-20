package pgutil

import "embed"

func NewEmbedMigrationReader(fs embed.FS) MigrationReader {
	return newFilesystemMigrationReader("<embed>", fs)
}
