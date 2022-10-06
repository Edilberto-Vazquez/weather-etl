package constants

import "regexp"

var (
	DateTimeRegexp  *regexp.Regexp = regexp.MustCompile(`\d\d/\d\d/\d\d\d\d\s\d\d:\d\d:\d\d`)
	LightningRegexp *regexp.Regexp = regexp.MustCompile(`Lightning Detected`)
	DistanceRegexp  *regexp.Regexp = regexp.MustCompile(`at\s\d\d\skm|at\s\d\skm`)
)
