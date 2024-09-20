package pgutil

import (
	"fmt"
	"regexp"
	"strings"
)

type Definition struct {
	ID            int
	Name          string
	UpQuery       Q
	DownQuery     Q
	IndexMetadata *IndexMetadata
}

type IndexMetadata struct {
	TableName string
	IndexName string
}

type MigrationReader interface {
	ReadAll() ([]RawDefinition, error)
}

type MigrationReaderFunc func() ([]RawDefinition, error)

func (f MigrationReaderFunc) ReadAll() ([]RawDefinition, error) {
	return f()
}

type RawDefinition struct {
	ID           int
	Name         string
	RawUpQuery   string
	RawDownQuery string
}

var (
	keyword = func(pattern string) string { return phrase(pattern) }
	phrase  = func(patterns ...string) string { return strings.Join(patterns, `\s+`) + `\s+` }
	opt     = func(pattern string) string { return `(?:` + pattern + `)?` }

	capturedIdentifierPattern          = `([a-zA-Z0-9$_]+|"(?:[^"]+)")`
	createIndexConcurrentlyPatternHead = strings.Join([]string{
		keyword(`CREATE`),
		opt(keyword(`UNIQUE`)),
		keyword(`INDEX`),
		opt(keyword(`CONCURRENTLY`)),
		opt(phrase(`IF`, `NOT`, `EXISTS`)),
		capturedIdentifierPattern, // capture index name
		`\s+`,
		keyword(`ON`),
		opt(keyword(`ONLY`)),
		capturedIdentifierPattern, // capture table name
	}, ``)

	createIndexConcurrentlyPattern    = regexp.MustCompile(createIndexConcurrentlyPatternHead)
	createIndexConcurrentlyPatternAll = regexp.MustCompile(createIndexConcurrentlyPatternHead + "[^;]+;")
)

func ReadMigrations(reader MigrationReader) (definitions []Definition, _ error) {
	rawDefinitions, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	ids := map[int]struct{}{}
	for _, rawDefinition := range rawDefinitions {
		if _, ok := ids[rawDefinition.ID]; ok {
			return nil, fmt.Errorf("duplicate migration identifier %d", rawDefinition.ID)
		}
		ids[rawDefinition.ID] = struct{}{}

		var indexMetadata *IndexMetadata
		prunedUp := removeComments(rawDefinition.RawUpQuery)
		prunedDown := removeComments(rawDefinition.RawDownQuery)

		if matches := createIndexConcurrentlyPattern.FindStringSubmatch(prunedUp); len(matches) > 0 {
			if strings.TrimSpace(createIndexConcurrentlyPatternAll.ReplaceAllString(prunedUp, "")) != "" {
				return nil, fmt.Errorf(`"create index concurrently" is not the only statement in the up migration`)
			}

			indexMetadata = &IndexMetadata{
				TableName: matches[2],
				IndexName: matches[1],
			}
		}

		if len(createIndexConcurrentlyPattern.FindAllString(prunedDown, 1)) > 0 {
			return nil, fmt.Errorf(`"create index concurrently" is not allowed in down migrations`)
		}

		definitions = append(definitions, Definition{
			ID:            rawDefinition.ID,
			Name:          rawDefinition.Name,
			UpQuery:       RawQuery(rawDefinition.RawUpQuery),
			DownQuery:     RawQuery(rawDefinition.RawDownQuery),
			IndexMetadata: indexMetadata,
		})
	}

	return definitions, nil
}

func removeComments(query string) string {
	var filtered []string
	for _, line := range strings.Split(query, "\n") {
		if line := strings.TrimSpace(strings.Split(line, "--")[0]); line != "" {
			filtered = append(filtered, line)
		}
	}

	return strings.TrimSpace(strings.Join(filtered, "\n"))
}
