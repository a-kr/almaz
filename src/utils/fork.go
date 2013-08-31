package utils
import (
	"log"
	"syscall"
)

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
