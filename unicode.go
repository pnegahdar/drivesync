package drivesync

// nfc preserves canonical equivalents without adding a runtime dependency.
// Normalization is used for collision and ignore matching, never identities.
func nfc(s string) string {
	ascii := true
	for i := 0; i < len(s); i++ {
		if s[i] >= 128 {
			ascii = false
			break
		}
	}
	if ascii {
		return s
	}
	var d []rune
	var decompose func(rune)
	decompose = func(c rune) {
		if c >= 0xac00 && c <= 0xd7a3 {
			n := c - 0xac00
			d = append(d, 0x1100+n/588, 0x1161+n%588/28)
			if t := n % 28; t != 0 {
				d = append(d, 0x11a7+t)
			}
			return
		}
		if parts, ok := canonicalDecomposition[c]; ok {
			for _, p := range parts {
				decompose(p)
			}
		} else {
			d = append(d, c)
		}
	}
	for _, c := range s {
		decompose(c)
	}
	for i := 1; i < len(d); i++ {
		class := canonicalClass[d[i]]
		if class == 0 {
			continue
		}
		for j := i; j > 0 && canonicalClass[d[j-1]] > class; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
	var out []rune
	starter := -1
	previous := 0
	for _, c := range d {
		class := canonicalClass[c]
		if starter >= 0 && (previous < class || previous == 0) {
			a := out[starter]
			combined, ok := canonicalComposition[[2]rune{a, c}]
			if a >= 0x1100 && a <= 0x1112 && c >= 0x1161 && c <= 0x1175 {
				combined = 0xac00 + (a-0x1100)*588 + (c-0x1161)*28
				ok = true
			}
			if a >= 0xac00 && a <= 0xd7a3 && (a-0xac00)%28 == 0 && c >= 0x11a8 && c <= 0x11c2 {
				combined = a + c - 0x11a7
				ok = true
			}
			if ok {
				out[starter] = combined
				continue
			}
		}
		if class == 0 {
			starter = len(out)
		}
		out = append(out, c)
		previous = class
	}
	return string(out)
}
