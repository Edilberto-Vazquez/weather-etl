package utils

import (
	"io/fs"
	"path/filepath"
)

func ReadDirectory(root string, fileType string) (files []string, err error) {
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(d.Name()) == "."+fileType {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return
}
