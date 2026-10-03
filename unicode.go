package drivesync

import (
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

func nfc(s string) string      { return norm.NFC.String(s) }
func foldPath(s string) string { return norm.NFC.String(cases.Fold().String(norm.NFC.String(s))) }
