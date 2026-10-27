package engine

import "syscall"

func statTimes(s *syscall.Stat_t) (mtime, ctime int64) {
	return s.Mtim.Nano(), s.Ctim.Nano()
}
