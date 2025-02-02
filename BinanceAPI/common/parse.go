package common

import (
	"errors"
	"fmt"
	"strconv"
)

func ParseFloat64FromAnyString(a any) (float64, error) {
	s, ok := a.(string)
	if !ok {
		return 0, errors.New("not string")
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse float from %q: %w", s, err)
	}
	return f, nil
}
