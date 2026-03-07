package main

import "goapplyjob-golang-backend/internal/watcher"

func main() {
	_ = watcher.New(watcher.Config{})
}
