package broker

import "testing"

func TestCompareIDs(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1-0", "1-0", 0},
		{"1-0", "2-0", -1},
		{"10-0", "9-99", 1},
		{"5-1", "5-10", -1},
		{"1700000000000-5", "1700000000000-3", 1},
	}
	for _, c := range cases {
		if got := compareIDs(c.a, c.b); got != c.want {
			t.Errorf("compareIDs(%s, %s) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}
