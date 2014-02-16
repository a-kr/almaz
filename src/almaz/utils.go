package main

import (
	"os"
	"path/filepath"
)

func GetExecutableDir() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return ""
	}
	return dir
}
