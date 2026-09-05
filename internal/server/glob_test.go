package server

import "testing"

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, str string
		want         bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"h*llo", "hello", true},
		{"h*llo", "heello", true}, // * matches the doubled 'e'
		{"h*llo", "hell", false},  // too short: no room for "llo"
		{"hell?", "hello", true},
		{"hell?", "hell", false},
		{"exact", "exact", true},
		{"exact", "exac", false},
		{"[abc]at", "bat", true},
		{"[abc]at", "dat", false},
		{"[^abc]at", "dat", true},
		{"[^abc]at", "bat", false},
		{"[a-z]est", "best", true},
		{"[a-z]est", "Best", false},
		{"user:*", "user:123", true},
		{"user:*", "order:1", false},
		{"a\\*b", "a*b", true},
		{"a\\*b", "axb", false},
		{"news.*", "news.ai", true},
		{"news.*", "sports", false},
		{"", "", true},
		{"", "x", false},
		{"*", "multi word string", true},
		{"a*b*c", "abc", true},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxbyy", false},
		{"[", "[", false}, // unterminated class: no match against non-']'... '[' alone in str
		{"[]]", "]", true},
		{"x[y", "x[y", false},
	}
	for _, tc := range cases {
		if got := globMatch(tc.pattern, tc.str); got != tc.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", tc.pattern, tc.str, got, tc.want)
		}
	}
}
