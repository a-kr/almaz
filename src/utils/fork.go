package utils
import (
	"log"
	"os"
	"syscall"
)

const (
	P_ALL = 0
)

func Wait() {
	_, _, errno := syscall.Syscall6(syscall.SYS_WAITID,
		P_ALL, 0, 0, syscall.WEXITED, 0, 0)
	if errno != 0 {
		log.Fatalf("Error while waiting: %d", errno)
	}
}

func Fork() int {
	childpid, _, errno := syscall.Syscall(syscall.SYS_FORK, 0, 0, 0)
	if errno != 0 {
		log.Fatalf("Error while forking: %d", errno)
	}
	if childpid > 0 {
		// parent
		return int(childpid)
	}
	return 0
}

func DoubleFork() int {
	f1 := Fork()
	if f1 > 0 {
		Wait()
		return f1
	} else {
		f2 := Fork()
		if f2 > 0 {
			os.Exit(0)
		}
	}
	return 0
}
