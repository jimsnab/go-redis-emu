package goredisemu

import "strings"

func redisGlob(pattern, candidate []rune) bool {

	if pattern == nil {
		return true
	}

	patPos := 0
	for i := 0; i < len(candidate); i++ {
		if patPos >= len(pattern) {
			return false
		}

		patCh := pattern[patPos]
		if patCh == '?' {
			patPos++
			continue
		} else if patCh == '*' {
			patPos++
			if patPos >= len(pattern) {
				return true
			}

			for j := i; j < len(candidate); j++ {
				if redisGlob(pattern[patPos:], candidate[j:]) {
					return true
				}
			}

			return false
		} else if patCh == '[' {
			var patSet strings.Builder
			patPos++
			for patPos < len(pattern) {
				letter := pattern[patPos]
				if letter == ']' {
					patPos++
					break
				}
				if letter == '\\' && patPos+1 < len(pattern) {
					patPos++
				}
				patSet.WriteRune(pattern[patPos])
				patPos++
			}
			if !strings.ContainsRune(patSet.String(), candidate[i]) {
				return false
			}
		} else if patCh == '\\' && patPos+1 < len(pattern) {
			patPos++
			if pattern[patPos] != candidate[i] {
				return false
			}
			patPos++
		} else {
			if patCh != candidate[i] {
				return false
			}
			patPos++
		}
	}

	for patPos < len(pattern) && pattern[patPos] == '*' {
		patPos++
	}

	return patPos >= len(pattern)
}
