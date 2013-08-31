// +build linux,386
package utils

import (
	"fmt"
	"log"
	"syscall"
)

const (
	SIG_IGN = 1
)


func Signal(signum syscall.Signal, handler int) error {
	log.Printf("SYS_SIGNAL = %d", syscall.SYS_SIGNAL)
	log.Printf("SYS_WAITID = %d", syscall.SYS_WAITID)
	_, _, e1 := syscall.Syscall(syscall.SYS_SIGNAL, uintptr(signum), uintptr(handler), 0)
	if e1 != 0 {
		return fmt.Errorf("Error on calling Signal: %d", e1)
	}
	return nil
}

func IgnoreDeadChildren() {
	err := Signal(syscall.SIGCHLD, SIG_IGN)
	if err != nil {
		log.Fatal(err)
	}
}
