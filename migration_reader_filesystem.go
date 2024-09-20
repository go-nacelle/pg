package pgutil

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type FilesystemMigrationReader struct {
	name string
	fs   fs.FS
}

func NewFilesystemMigrationReader(dirname string) MigrationReader {
	return newFilesystemMigrationReader(dirname, os.DirFS(dirname))
}

func newFilesystemMigrationReader(name string, fs fs.FS) MigrationReader {
	return &FilesystemMigrationReader{
		name: name,
		fs:   fs,
	}
}

func (r *FilesystemMigrationReader) ReadAll() (definitions []RawDefinition, _ error) {
	root, err := http.FS(r.fs).Open("/")
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("migration directory %q does not exist", r.name)
		}

		return nil, err
	}
	defer root.Close()

	entries, err := root.Readdir(0)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			if definition, ok, err := r.readDefinition(entry.Name()); err != nil {
				return nil, err
			} else if ok {
				definitions = append(definitions, definition)
			}
		}
	}

	sort.Slice(definitions, func(i, j int) bool { return definitions[i].ID < definitions[j].ID })
	return definitions, nil
}

func (r *FilesystemMigrationReader) readDefinition(dirname string) (RawDefinition, bool, error) {
	upPath := path.Join(dirname, "up.sql")
	downPath := path.Join(dirname, "down.sql")

	upFileContents, upErr := readFile(r.fs, upPath)
	downFileContents, downErr := readFile(r.fs, downPath)
	if os.IsNotExist(upErr) && os.IsNotExist(downErr) {
		return RawDefinition{}, false, nil
	} else if upErr != nil {
		return RawDefinition{}, false, upErr
	} else if downErr != nil {
		return RawDefinition{}, false, downErr
	}

	nameParts := strings.SplitN(dirname, "_", 2)
	id, err := strconv.Atoi(nameParts[0])
	if err != nil {
		return RawDefinition{}, false, err
	}
	name := strings.Replace(nameParts[1], "_", " ", -1)

	definition := RawDefinition{
		ID:           id,
		Name:         name,
		RawUpQuery:   string(upFileContents),
		RawDownQuery: string(downFileContents),
	}

	return definition, true, nil
}

func readFile(fs fs.FS, filepath string) ([]byte, error) {
	file, err := fs.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	contents, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return contents, nil
}
