package workerlog

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultLogDir = "logs"
	envLogDir     = "LOG_DIR"
)

func Setup(logFileEnv, defaultLogFile string) (func() error, error) {
	logDir := strings.TrimSpace(os.Getenv(envLogDir))
	if logDir == "" {
		logDir = defaultLogDir
	}
	logFile := strings.TrimSpace(os.Getenv(logFileEnv))
	if logFile == "" {
		logFile = defaultLogFile
	}

	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}

	filePath := filepath.Join(logDir, logFile)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	log.SetOutput(io.MultiWriter(os.Stdout, file))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	return file.Close, nil
}
