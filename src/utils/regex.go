package utils

import (
	"errors"
	"regexp"
)

func FindString(s string, r *regexp.Regexp) (string, error) {
	if !r.MatchString(s) {
		return "", errors.New("regular expression does not match")
	}
	return r.FindString(s), nil
}
