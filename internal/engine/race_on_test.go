//go:build race

package engine

// raceSlowdown scales wall-clock budgets: the race detector makes row decoding
// many times slower, so absolute limits only bound scaling, not speed.
const raceSlowdown = 10
