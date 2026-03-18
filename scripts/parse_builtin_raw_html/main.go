package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"goapplyjob-golang-backend/internal/sources/builtin"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: parse_builtin_raw_html <html_path>")
		os.Exit(2)
	}
	htmlPath := os.Args[1]
	htmlBytes, err := os.ReadFile(htmlPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", htmlPath, err)
		os.Exit(1)
	}
	payload := builtin.ExtractJobFromHTML(string(htmlBytes), "")
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal payload: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("source_html=%s\n", filepath.Base(htmlPath))
	fmt.Println(string(encoded))
}
