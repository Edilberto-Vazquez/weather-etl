package extract

import (
	"path/filepath"

	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

type ExtractedFile struct {
	fileName string
	lines    []string
}

func ExtractFile(filePath string) (*ExtractedFile, error) {
	lines, err := utils.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return &ExtractedFile{
		fileName: filepath.Base(filePath),
		lines:    lines,
	}, nil
}

func (efmFile *ExtractedFile) GetFileName() string {
	return efmFile.fileName
}

func (efmFile *ExtractedFile) GetLines() []string {
	return efmFile.lines
}
