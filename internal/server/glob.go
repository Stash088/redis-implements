package server

// globMatch reports whether str matches pattern using Redis glob rules:
// '*' matches any sequence of characters, '?' any single character,
// "[...]" a character class (with ranges and a leading '^' for negation)
// and '\\' escapes the next character.
func globMatch(pattern, str string) bool {
	var p, s int
	star := -1 // last '*' position in pattern
	ss := 0    // str position when the star was seen
	for s < len(str) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				star = p
				p++
				ss = s
				continue
			case '?':
				p++
				s++
				continue
			case '[':
				if ok, np := matchClass(pattern, p, str[s]); ok {
					p = np
					s++
					continue
				}
			case '\\':
				if p+1 < len(pattern) && pattern[p+1] == str[s] {
					p += 2
					s++
					continue
				}
			default:
				if pattern[p] == str[s] {
					p++
					s++
					continue
				}
			}
		}
		// Mismatch: backtrack to the last '*' and let it eat one more char.
		if star >= 0 {
			p = star + 1
			ss++
			s = ss
			continue
		}
		return false
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchClass matches ch against the character class starting at pattern[p]
// (pattern[p] must be '['). It returns whether ch matched and the index just
// past the closing ']'. An unterminated class is matched to the end of the
// pattern.
func matchClass(pattern string, p int, ch byte) (bool, int) {
	p++ // skip '['
	not := false
	if p < len(pattern) && pattern[p] == '^' {
		not = true
		p++
	}
	matched := false
	first := true
	for p < len(pattern) {
		if pattern[p] == ']' && !first {
			return matched != not, p + 1
		}
		first = false
		if pattern[p] == '\\' && p+1 < len(pattern) {
			if pattern[p+1] == ch {
				matched = true
			}
			p += 2
			continue
		}
		// Range "[a-z]"; a trailing '-' before ']' is a literal.
		if p+2 < len(pattern) && pattern[p+1] == '-' && pattern[p+2] != ']' {
			if ch >= pattern[p] && ch <= pattern[p+2] {
				matched = true
			}
			p += 3
			continue
		}
		if pattern[p] == ch {
			matched = true
		}
		p++
	}
	return matched != not, p
}
