package pgutil

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Q struct {
	internalFormat    string
	replacerPairs     []string
	parameterizedArgs []any
}

type Args map[string]any

func Query(format string, args Args) Q {
	var (
		internalFormat      string
		replacerPairs       []string
		parameterizedArgs   []any
		previousIndex       = 0
		placeholdersToIndex = map[string]int{}
	)

	for _, part := range tokenize(format) {
		name, ok := extractPlaceholderName(part)
		if !ok {
			// literal, not a placeholder
			internalFormat += part
			continue
		}

		value, ok := args[name]
		if !ok {
			panic(fmt.Sprintf("no arg supplied for %q", name))
		}

		if q, ok := value.(Q); ok {
			// Serialize all internal placeholders transforming `{$X}` -> `{${X+lastIndex}}`
			subInternalFormat, subReplacerPairs, subParameterizedArgs := q.bumpPlaceholderIndices(previousIndex)

			// Embed this query into the internal format
			internalFormat += subInternalFormat
			replacerPairs = append(replacerPairs, subReplacerPairs...)
			parameterizedArgs = append(parameterizedArgs, subParameterizedArgs...)

			// Bump indexes by number of placeholders in serialized query
			previousIndex += len(subReplacerPairs) / 2
		} else {
			// Re-use parameter if possible; otherwise create a new placeholder
			index, ok := placeholdersToIndex[name]
			if !ok {
				previousIndex++
				index = previousIndex
				placeholdersToIndex[name] = index
				parameterizedArgs = append(parameterizedArgs, value)
			}

			placeholder := fmt.Sprintf("$%d", index)
			internalPlaceholder := fmt.Sprintf("{%s}", placeholder)

			// Embed placeholder into the internal format
			internalFormat += internalPlaceholder
			replacerPairs = append(replacerPairs, internalPlaceholder, placeholder)
		}
	}

	return Q{
		internalFormat:    internalFormat,
		replacerPairs:     replacerPairs,
		parameterizedArgs: parameterizedArgs,
	}
}

func Quote(format string) Q {
	return RawQuery(format)
}

func RawQuery(format string, args ...any) Q {
	return Q{internalFormat: format, parameterizedArgs: args}
}

func queryf(format string, args ...any) Q {
	return RawQuery(fmt.Sprintf(format, args...))
}

func (q Q) Format() (string, []any) {
	return replaceWithPairs(q.internalFormat, q.replacerPairs...), q.parameterizedArgs
}

func (q Q) bumpPlaceholderIndices(offset int) (string, []string, []any) {
	var (
		rewriterPairs = make([]string, 0, len(q.replacerPairs))
		replacerPairs = make([]string, 0, len(q.replacerPairs))
	)

	for i := 0; i < len(q.replacerPairs); i += 2 {
		oldPlaceholder := q.replacerPairs[i]
		oldPlaceholderValue, _ := strconv.Atoi(q.replacerPairs[i+1][1:])

		newPlaceholder := fmt.Sprintf("$%d", oldPlaceholderValue+offset)
		newInternalPlaceholder := fmt.Sprintf("{%s}", newPlaceholder)

		rewriterPairs = append(rewriterPairs, oldPlaceholder, newInternalPlaceholder)
		replacerPairs = append(replacerPairs, newInternalPlaceholder, newPlaceholder)
	}

	return replaceWithPairs(q.internalFormat, rewriterPairs...), replacerPairs, q.parameterizedArgs
}

var placeholderPattern = regexp.MustCompile(`{:(\w+)}`)

func tokenize(format string) []string {
	var (
		matches = placeholderPattern.FindAllStringIndex(format, -1)
		parts   = make([]string, 0, len(matches)*2+1)
		offset  = 0
	)

	for _, match := range matches {
		parts = append(parts, format[offset:match[0]])   // capture from last match up to this placeholder
		parts = append(parts, format[match[0]:match[1]]) // capture `{:placeholder}`
		offset = match[1]
	}

	// capture from last match to end of string
	// NOTE: if there were no matches offset will be zero
	return append(parts, format[offset:])
}

func extractPlaceholderName(part string) (string, bool) {
	matches := placeholderPattern.FindStringSubmatch(part)
	if len(matches) > 0 {
		return matches[1], true
	}

	return "", false
}

func replaceWithPairs(format string, replacerPairs ...string) string {
	return strings.NewReplacer(replacerPairs...).Replace(format)
}
