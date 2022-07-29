package utils

import (
	"bufio"
	"os"
)

func ReadFile(path string) ([]string, error) {
	lines := make([]string, 0)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, file.Close()
}
