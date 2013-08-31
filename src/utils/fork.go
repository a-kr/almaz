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
	log.Printf("fork one...")
	f1 := Fork()
	if f1 > 0 {
		log.Printf("one forked, im parent, waiting")
		Wait()
		log.Printf("parent wait complete")
		return f1
	} else {
		log.Printf("im child, fork two")
		f2 := Fork()
		if f2 > 0 {
			log.Printf("im parent of child, dying")
			os.Exit(0)
		}
	}
	log.Printf("im grandchild, living")
	return 0
}
