package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"goapplyjob-golang-backend/internal/sources/remotedotco"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: parse_remotedotco_raw_html <path-to-html>")
		os.Exit(2)
	}
	inputPath := os.Args[1]
	raw, err := os.ReadFile(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read html: %v\n", err)
		os.Exit(1)
	}
	payload, err := remotedotco.ParseRawHTML(string(raw), inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse remotedotco html: %v\n", err)
		os.Exit(1)
	}
	out, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal json: %v\n", err)
		os.Exit(1)
	}
	base := strings.TrimSuffix(filepath.Base(inputPath), filepath.Ext(inputPath))
	outPath := filepath.Join(filepath.Dir(inputPath), base+".generated.json")
	if err := os.WriteFile(outPath, append(out, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write json: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s\n", outPath)
}
